using Fugu.Core.Actors;
using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

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
        var allocateWriteBatchChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var writeWriteBatchChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var updateIndexChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var indexUpdatedChannel = Channel.CreateBounded<DummyMessage>(new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        var snapshotsUpdatedChannel = Channel.CreateBounded<DummyMessage>(new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        var getSnapshotChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var releaseSnapshotChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);

        var updateSegmentStatsChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var segmentStatsUpdatedChannel = Channel.CreateBounded<DummyMessage>(new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        var segmentEmptiedChannel = Channel.CreateUnbounded<DummyMessage>();
        var segmentEvictedChannel = Channel.CreateUnbounded<DummyMessage>();

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
