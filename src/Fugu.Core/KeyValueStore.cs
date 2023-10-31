using Fugu.Actors;
using Fugu.Channels;
using Fugu.IO;
using System.Threading.Channels;

namespace Fugu;

public sealed class KeyValueStore : IAsyncDisposable
{
    private readonly IBackingStorage _storage;
    private readonly AllocationActor _allocationActor;
    private readonly WriterActor _writerActor;
    private readonly IndexActor _indexActor;
    private readonly SnapshotsActor _snapshotsActor;
    private readonly CompactionActor _compactionActor;

    private Task? _runTask;

    private KeyValueStore(
        IBackingStorage storage,
        AllocationActor allocationActor,
        WriterActor writerActor,
        IndexActor indexActor,
        SnapshotsActor snapshotsActor,
        CompactionActor compactionActor)
    {
        _storage = storage;
        _allocationActor = allocationActor;
        _writerActor = writerActor;
        _indexActor = indexActor;
        _snapshotsActor = snapshotsActor;
        _compactionActor = compactionActor;
    }

    public static ValueTask<KeyValueStore> CreateAsync(IBackingStorage storage)
    {
        // Create channels
        var changeSetAllocatedChannel = Channel.CreateUnbounded<ChangeSetAllocated>();
        var changesWrittenChannel = Channel.CreateUnbounded<ChangesWritten>();
        var indexUpdatedChannel = Channel.CreateUnbounded<IndexUpdated>();

        // Create actors
        var allocationActor = new AllocationActor(storage, changeSetAllocatedChannel);
        var writerActor = new WriterActor(changeSetAllocatedChannel, changesWrittenChannel);
        var indexActor = new IndexActor(changesWrittenChannel, indexUpdatedChannel);
        var snapshotsActor = new SnapshotsActor(indexUpdatedChannel);
        var compactionActor = new CompactionActor();

        var store = new KeyValueStore(
            storage,
            allocationActor,
            writerActor,
            indexActor,
            snapshotsActor,
            compactionActor);

        store.Start();

        return ValueTask.FromResult(store);
    }

    public async ValueTask DisposeAsync()
    {
        if (_runTask is not null)
        {
            await _runTask;
        }

        throw new NotImplementedException();
    }

    public ValueTask<Snapshot> GetSnapshotAsync()
    {
        throw new NotImplementedException();
    }

    public async ValueTask SaveAsync(ChangeSet changeSet)
    {
        var clock = await _allocationActor.EnqueueChangeSetAsync(changeSet);
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
