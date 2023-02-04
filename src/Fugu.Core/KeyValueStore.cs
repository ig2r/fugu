using Fugu.Core.Actors;

namespace Fugu.Core;

public sealed class KeyValueStore : IAsyncDisposable
{
    private readonly AllocationActor _allocationActor;
    private readonly WriterActor _writerActor;
    private readonly IndexActor _indexActor;
    private readonly SnapshotsActor _snapshotsActor;
    private readonly SegmentStatsActor _segmentStatsActor;
    private readonly CompactionActor _compactionActor;

    public KeyValueStore(
        AllocationActor allocationActor,
        WriterActor writerActor,
        IndexActor indexActor,
        SnapshotsActor snapshotsActor,
        SegmentStatsActor segmentStatsActor,
        CompactionActor compactionActor)
    {
        _allocationActor = allocationActor;
        _writerActor = writerActor;
        _indexActor = indexActor;
        _snapshotsActor = snapshotsActor;
        _segmentStatsActor = segmentStatsActor;
        _compactionActor = compactionActor;
    }

    public static ValueTask<KeyValueStore> CreateAsync(TableSet tableSet)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask<Snapshot> GetSnapshotAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask WriteAsync(WriteBatch batch)
    {
        throw new NotImplementedException();
    }
}
