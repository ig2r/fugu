using Fugu.Actors;
using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu;

public sealed class KeyValueStore : IAsyncDisposable
{
    private readonly AllocationActor _allocationActor;
    private readonly WriterActor _writerActor;
    private readonly IndexActor _indexActor;
    private readonly SnapshotsActor _snapshotsActor;
    private readonly CompactionActor _compactionActor;

    private Task? _runTask;
    private bool _disposed;

    private KeyValueStore(
        AllocationActor allocationActor,
        WriterActor writerActor,
        IndexActor indexActor,
        SnapshotsActor snapshotsActor,
        CompactionActor compactionActor)
    {
        _allocationActor = allocationActor;
        _writerActor = writerActor;
        _indexActor = indexActor;
        _snapshotsActor = snapshotsActor;
        _compactionActor = compactionActor;
    }

    public static async Task<KeyValueStore> CreateAsync(IBackingStorage storage)
    {
        const int defaultCapacity = 512;

        // Create channels
        var changeSetAllocatedChannel = Channel.CreateBounded<ChangeSetAllocated>(new BoundedChannelOptions(defaultCapacity)
        {
            AllowSynchronousContinuations = true,
            SingleWriter = true,
            SingleReader = true,
        });

        var changesWrittenChannel = Channel.CreateBounded<ChangesWritten>(new BoundedChannelOptions(defaultCapacity)
        {
            AllowSynchronousContinuations = true,
            SingleWriter = true,
            SingleReader = true,
        });

        // Single-element channel because CompactionActor waits for effects of a compaction to become observable before
        // starting the next compaction.
        var compactionWrittenChannel = Channel.CreateBounded<CompactionWritten>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = false,
            SingleWriter = true,
            SingleReader = true,
        });

        var indexUpdatedChannel = Channel.CreateBounded<IndexUpdated>(new BoundedChannelOptions(defaultCapacity)
        {
            AllowSynchronousContinuations = true,
            SingleWriter = true,
            SingleReader = true,
        });

        var segmentStatsUpdatedChannel = Channel.CreateBounded<SegmentStatsUpdated>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.DropNewest,
            SingleWriter = true,
            SingleReader = true,
        });

        var oldestObservableSnapshotChangedChannel = Channel.CreateBounded<OldestObservableSnapshotChanged>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.DropNewest,
            SingleWriter = true,
            SingleReader = true,
        });

        // Unbounded because this channel sees little traffic, yet serves as an "elasticity escape hatch" to rule out the
        // possibility of deadlocking the actor mesh if channel messages should ever back up.
        var segmentsCompactedChannel = Channel.CreateUnbounded<SegmentsCompacted>(new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = false,
            SingleWriter = true,
            SingleReader = true,
        });

        // Create actors involved in bootstrapping
        var indexActor = new IndexActor(
            changesWrittenChannel.Reader,
            compactionWrittenChannel.Reader,
            indexUpdatedChannel.Writer,
            segmentStatsUpdatedChannel.Writer);

        var snapshotsActor = new SnapshotsActor(
            indexUpdatedChannel.Reader,
            oldestObservableSnapshotChangedChannel.Writer);

        // Load existing data
        var bootstrapResult = await Bootstrapper.InitializeStoreAsync(storage, changesWrittenChannel);

        var balancingStrategy = new BalancingStrategy(100, 1.5);

        // Create actors involved in writes and balancing
        var allocationActor = new AllocationActor(
            storage,
            balancingStrategy,
            segmentsCompactedChannel.Reader,
            changeSetAllocatedChannel.Writer,
            bootstrapResult.TotalBytes);

        var writerActor = new WriterActor(
            changeSetAllocatedChannel.Reader,
            changesWrittenChannel.Writer,
            bootstrapResult.MaxGeneration);

        var compactionActor = new CompactionActor(
            storage,
            balancingStrategy,
            segmentStatsUpdatedChannel.Reader,
            oldestObservableSnapshotChangedChannel.Reader,
            compactionWrittenChannel.Writer,
            segmentsCompactedChannel.Writer);

        var store = new KeyValueStore(
            allocationActor,
            writerActor,
            indexActor,
            snapshotsActor,
            compactionActor);

        store.Start();

        return store;
    }

    /// <summary>
    /// Shuts down the key-value store gracefully.
    /// </summary>
    /// <returns>Awaitable that signals when shutdown is completed.</returns>
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _allocationActor.CompleteAsync();
        }

        if (_runTask is not null)
        {
            await _runTask;
        }
    }

    public ValueTask<Snapshot> GetSnapshotAsync()
    {
        return _snapshotsActor.GetSnapshotAsync();
    }

    public async ValueTask SaveAsync(ChangeSet changeSet)
    {
        // Ask allocation actor to persist this change set, receive back the vector clock value
        // associated with the write operation. Then, stall until the effects of that write become
        // observable in snapshots from the store.
        var clock = await _allocationActor.EnqueueChangeSetAsync(changeSet);
        await _snapshotsActor.WaitForObservableEffectsAsync(clock);
    }

    private void Start()
    {
        _runTask = Task.WhenAll(
            _allocationActor.RunAsync(),
            _writerActor.RunAsync(),
            _indexActor.RunAsync(),
            _snapshotsActor.RunAsync(),
            _compactionActor.RunAsync());
    }
}
