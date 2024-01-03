using Fugu.Actors;
using Fugu.Channels;
using Fugu.IO;
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

    public static async ValueTask<KeyValueStore> CreateAsync(IBackingStorage storage)
    {
        // Create channels
        var changeSetAllocatedChannel = Channel.CreateUnbounded<ChangeSetAllocated>();
        var changesWrittenChannel = Channel.CreateUnbounded<ChangesWritten>();
        var indexUpdatedChannel = Channel.CreateUnbounded<IndexUpdated>();
        var segmentStatsUpdatedChannel = Channel.CreateBounded<SegmentStatsUpdated>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropNewest,
        });

        // Create actors involved in bootstrapping
        var indexActor = new IndexActor(changesWrittenChannel, indexUpdatedChannel, segmentStatsUpdatedChannel);
        var snapshotsActor = new SnapshotsActor(indexUpdatedChannel);

        // Load existing data
        var bootstrapResult = await Bootstrapper.LoadFromStorageAsync(storage, changesWrittenChannel);

        // Create actors involved in writes and balancing
        var allocationActor = new AllocationActor(storage, changeSetAllocatedChannel, bootstrapResult.TotalBytes);
        var writerActor = new WriterActor(changeSetAllocatedChannel, changesWrittenChannel, bootstrapResult.MaxGeneration);
        var compactionActor = new CompactionActor(segmentStatsUpdatedChannel);

        var store = new KeyValueStore(
            allocationActor,
            writerActor,
            indexActor,
            snapshotsActor,
            compactionActor);

        store.Start();

        return store;
    }

    public async ValueTask DisposeAsync()
    {
        // TODO: currently, this will throw when disposing multiple times; should not throw
        await _allocationActor.CompleteAsync();

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
