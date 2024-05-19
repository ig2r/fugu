using Fugu.Actors;
using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu;

public sealed class KeyValueStore : IAsyncDisposable
{
    private readonly Task _runTask;
    private readonly AllocationActor _allocationActor;
    private readonly SnapshotsActor _snapshotsActor;

    private bool _disposed;

    private KeyValueStore(
        Task runTask,
        AllocationActor allocationActor,
        SnapshotsActor snapshotsActor)
    {
        _runTask = runTask;
        _allocationActor = allocationActor;
        _snapshotsActor = snapshotsActor;
    }

    public static async Task<KeyValueStore> CreateAsync(IBackingStorage storage)
    {
        const int defaultCapacity = 512;

        // Create channels used to pass messages between actors.
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

        var indexUpdatedChannel = Channel.CreateBounded<IndexUpdated>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = true,
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleWriter = true,
            SingleReader = true,
        });

        var segmentStatsUpdatedChannel = Channel.CreateBounded<SegmentStatsUpdated>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleWriter = true,
            SingleReader = true,
        });

        var oldestObservableSnapshotChangedChannel = Channel.CreateBounded<OldestObservableSnapshotChanged>(new BoundedChannelOptions(1)
        {
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.DropOldest,
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

        // Index actor must be running during bootstrapping because it needs to build the index from the change sets
        // that the bootstrapper reads from storage.
        var indexActor = new IndexActor(
            changesWrittenChannel.Reader,
            compactionWrittenChannel.Reader,
            indexUpdatedChannel.Writer,
            segmentStatsUpdatedChannel.Writer);

        var indexActorRunTask = indexActor.RunAsync();

        // Load existing data.
        var bootstrapResult = await Bootstrapper.InitializeStoreAsync(storage, changesWrittenChannel);

        // Create actors involved in writes and balancing.
        var balancingStrategy = new BalancingStrategy(100, 1.5);

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

        var snapshotsActor = new SnapshotsActor(
            indexUpdatedChannel.Reader,
            oldestObservableSnapshotChangedChannel.Writer);

        var compactionActor = new CompactionActor(
            storage,
            balancingStrategy,
            segmentStatsUpdatedChannel.Reader,
            oldestObservableSnapshotChangedChannel.Reader,
            compactionWrittenChannel.Writer,
            segmentsCompactedChannel.Writer);

        // Start all remaining actors and construct a single compound task that will complete when
        // all individual actors have terminated.
        var runTask = Task.WhenAll(
            allocationActor.RunAsync(),
            writerActor.RunAsync(),
            indexActorRunTask,
            snapshotsActor.RunAsync(),
            compactionActor.RunAsync());

        return new KeyValueStore(runTask, allocationActor, snapshotsActor);
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

    /// <summary>
    /// Acquires a consistent snapshot of all keys and values in the store.
    /// </summary>
    /// <remarks>
    /// Ensure to dispose the snapshot after use and do not keep a snapshot open for extended
    /// periods of time.
    /// </remarks>
    /// <returns>Snapshot reflecting the current state of data in the store.</returns>
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
}
