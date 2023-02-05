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
    private readonly ChannelWriter<DummyMessage> _allocateWriteBatchChannelWriter;
    private readonly ChannelWriter<DummyMessage> _getSnapshotChannelWriter;
    private readonly ChannelWriter<DummyMessage> _releaseSnapshotChannelWriter;
    private readonly ChannelWriter<DummyMessage> _awaitClockChannelWriter;
    private readonly Task _allActorsCompletion;

    public KeyValueStore(
        AllocationActor allocationActor,
        WriterActor writerActor,
        IndexActor indexActor,
        SnapshotsActor snapshotsActor,
        SegmentStatsActor segmentStatsActor,
        CompactionActor compactionActor,
        ChannelWriter<DummyMessage> allocateWriteBatchChannelWriter,
        ChannelWriter<DummyMessage> getSnapshotChannelWriter,
        ChannelWriter<DummyMessage> releaseSnapshotChannelWriter,
        ChannelWriter<DummyMessage> awaitClockChannelWriter,
        Task allActorsCompletion)
    {
        _allocationActor = allocationActor;
        _writerActor = writerActor;
        _indexActor = indexActor;
        _snapshotsActor = snapshotsActor;
        _segmentStatsActor = segmentStatsActor;
        _compactionActor = compactionActor;
        _allocateWriteBatchChannelWriter = allocateWriteBatchChannelWriter;
        _getSnapshotChannelWriter = getSnapshotChannelWriter;
        _releaseSnapshotChannelWriter = releaseSnapshotChannelWriter;
        _awaitClockChannelWriter = awaitClockChannelWriter;
        _allActorsCompletion = allActorsCompletion;
    }

    public static ValueTask<KeyValueStore> CreateAsync(TableSet tableSet)
    {
        // Create channels for message-passing between actors
        var dropOldest = new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        };

        var allocateWriteBatchChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var writeWriteBatchChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var updateIndexChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var indexUpdatedChannel = Channel.CreateBounded<DummyMessage>(dropOldest);
        var snapshotsUpdatedChannel = Channel.CreateBounded<DummyMessage>(dropOldest);

        var awaitClockChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var getSnapshotChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var releaseSnapshotChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);

        var updateSegmentStatsChannel = Channel.CreateBounded<DummyMessage>(capacity: 1);
        var segmentStatsUpdatedChannel = Channel.CreateBounded<DummyMessage>(dropOldest);

        var segmentEmptiedChannel = Channel.CreateUnbounded<DummyMessage>();
        var segmentEvictedChannel = Channel.CreateUnbounded<DummyMessage>();

        // Create actors
        var allocationActor = new AllocationActor(
            allocateWriteBatchChannel.Reader,
            segmentEvictedChannel.Reader,
            writeWriteBatchChannel.Writer);

        var writerActor = new WriterActor(
            writeWriteBatchChannel.Reader,
            updateIndexChannel.Writer);

        var indexActor = new IndexActor(
            updateIndexChannel.Reader,
            indexUpdatedChannel.Writer,
            updateSegmentStatsChannel.Writer);

        var snapshotsActor = new SnapshotsActor(
            indexUpdatedChannel.Reader,
            awaitClockChannel.Reader,
            getSnapshotChannel.Reader,
            releaseSnapshotChannel.Reader,
            snapshotsUpdatedChannel.Writer);

        var segmentStatsActor = new SegmentStatsActor(
            updateSegmentStatsChannel.Reader,
            segmentStatsUpdatedChannel.Writer,
            segmentEmptiedChannel.Writer);

        var compactionActor = new CompactionActor(
            segmentStatsUpdatedChannel.Reader,
            segmentEmptiedChannel.Reader,
            snapshotsUpdatedChannel.Reader,
            updateIndexChannel.Writer,
            segmentEvictedChannel.Writer);

        // Start bootstrap actors, load data, then start all remaining actors
        var bootstrapActorsCompletion = Task.WhenAll(
            indexActor.RunAsync(),
            segmentStatsActor.RunAsync());

        // TODO: enumerate tables in table set and populate index

        var allActorsCompletion = Task.WhenAll(
            bootstrapActorsCompletion,
            allocationActor.RunAsync(),
            writerActor.RunAsync(),
            snapshotsActor.RunAsync(),
            compactionActor.RunAsync());

        var store = new KeyValueStore(
            allocationActor,
            writerActor,
            indexActor,
            snapshotsActor,
            segmentStatsActor,
            compactionActor,
            allocateWriteBatchChannel.Writer,
            getSnapshotChannel.Writer,
            releaseSnapshotChannel.Writer,
            awaitClockChannel.Writer,
            allActorsCompletion);

        return ValueTask.FromResult(store);
    }

    public async ValueTask DisposeAsync()
    {
        _allocateWriteBatchChannelWriter.Complete();
        _getSnapshotChannelWriter.Complete();
        _releaseSnapshotChannelWriter.Complete();
        _awaitClockChannelWriter.Complete();

        await _allActorsCompletion;
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
