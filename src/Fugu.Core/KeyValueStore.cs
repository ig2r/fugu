using Fugu.Core.Actors;
using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
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
    private readonly ChannelWriter<AllocateWriteBatchMessage> _allocateWriteBatchChannelWriter;
    private readonly ChannelWriter<AcquireSnapshotMessage> _acquireSnapshotChannelWriter;
    private readonly ChannelWriter<DummyMessage> _releaseSnapshotChannelWriter;
    private readonly ChannelWriter<AwaitClockMessage> _awaitClockChannelWriter;
    private readonly Task _allActorsCompletion;

    public KeyValueStore(
        AllocationActor allocationActor,
        WriterActor writerActor,
        IndexActor indexActor,
        SnapshotsActor snapshotsActor,
        SegmentStatsActor segmentStatsActor,
        CompactionActor compactionActor,
        ChannelWriter<AllocateWriteBatchMessage> allocateWriteBatchChannelWriter,
        ChannelWriter<AcquireSnapshotMessage> acquireSnapshotChannelWriter,
        ChannelWriter<DummyMessage> releaseSnapshotChannelWriter,
        ChannelWriter<AwaitClockMessage> awaitClockChannelWriter,
        Task allActorsCompletion)
    {
        _allocationActor = allocationActor;
        _writerActor = writerActor;
        _indexActor = indexActor;
        _snapshotsActor = snapshotsActor;
        _segmentStatsActor = segmentStatsActor;
        _compactionActor = compactionActor;
        _allocateWriteBatchChannelWriter = allocateWriteBatchChannelWriter;
        _acquireSnapshotChannelWriter = acquireSnapshotChannelWriter;
        _releaseSnapshotChannelWriter = releaseSnapshotChannelWriter;
        _awaitClockChannelWriter = awaitClockChannelWriter;
        _allActorsCompletion = allActorsCompletion;
    }

    public static ValueTask<KeyValueStore> CreateAsync(TableSet tableSet)
    {
        // Create channels for message-passing between actors
        var defaultBounded = new BoundedChannelOptions(capacity: 1)
        {
            AllowSynchronousContinuations = true,
            SingleReader = true,
        };

        var dropOldest = new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            AllowSynchronousContinuations = true,
            SingleReader = true,
        };

        var dropOldestAsync = new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            AllowSynchronousContinuations = false,
            SingleReader = true,
        };

        var allocateWriteBatchChannel = Channel.CreateBounded<AllocateWriteBatchMessage>(defaultBounded);
        var writeWriteBatchChannel = Channel.CreateBounded<WriteWriteBatchMessage>(defaultBounded);

        // TODO: Both writer and compaction actors currently write to this channel.
        // This requires us to be extra careful during shutdown, as writer actor may have
        // already completed this channel when compaction actor tries to write to it.
        // Consider running two separate channels instead?
        var updateIndexChannel = Channel.CreateBounded<UpdateIndexMessage>(defaultBounded);
        var indexUpdatedChannel = Channel.CreateBounded<IndexUpdatedMessage>(dropOldest);
        var snapshotsUpdatedChannel = Channel.CreateBounded<DummyMessage>(dropOldestAsync);

        var awaitClockChannel = Channel.CreateBounded<AwaitClockMessage>(defaultBounded);
        var acquireSnapshotChannel = Channel.CreateBounded<AcquireSnapshotMessage>(defaultBounded);
        var releaseSnapshotChannel = Channel.CreateBounded<DummyMessage>(defaultBounded);

        var updateSegmentStatsChannel = Channel.CreateBounded<UpdateSegmentStatsMessage>(defaultBounded);
        var segmentStatsUpdatedChannel = Channel.CreateBounded<SegmentStatsUpdatedMessage>(dropOldestAsync);

        var segmentEmptiedChannel = Channel.CreateUnbounded<DummyMessage>();
        var segmentEvictedChannel = Channel.CreateUnbounded<DummyMessage>();

        // Create actors
        var allocationActor = new AllocationActor(
            allocateWriteBatchChannel.Reader,
            segmentEvictedChannel.Reader,
            writeWriteBatchChannel.Writer,
            tableSet);

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
            acquireSnapshotChannel.Reader,
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
            acquireSnapshotChannel.Writer,
            releaseSnapshotChannel.Writer,
            awaitClockChannel.Writer,
            allActorsCompletion);

        return ValueTask.FromResult(store);
    }

    public ValueTask DisposeAsync()
    {
        // Complete all channels that the store holds into the actor mesh. Receiving
        // actors will propagate completion among themselves, shutting down all actors
        // in the mesh.
        _allocateWriteBatchChannelWriter.Complete();
        _acquireSnapshotChannelWriter.Complete();
        _releaseSnapshotChannelWriter.Complete();
        _awaitClockChannelWriter.Complete();

        // Wait until all actors have completed
        return new ValueTask(_allActorsCompletion);
    }

    public async ValueTask<Snapshot> GetSnapshotAsync()
    {
        var replyChannel = Channel.CreateBounded<Snapshot>(capacity: 1);

        var message = new AcquireSnapshotMessage
        {
            ReplyChannelWriter = replyChannel.Writer,
        };

        await _acquireSnapshotChannelWriter.WriteAsync(message);
        var snapshot = await replyChannel.Reader.ReadAsync();

        return snapshot;
    }

    public async ValueTask WriteAsync(WriteBatch batch)
    {
        // TODO: creating channels is costly. Pool these (consider ObjectPool<T>) and/or
        // switch to ManualResetValueTaskSourceCore, possibly pooled.
        var replyChannel = Channel.CreateBounded<VectorClock>(capacity: 1);

        var message = new AllocateWriteBatchMessage
        {
            Batch = batch,
            ReplyChannelWriter = replyChannel.Writer,
        };

        await _allocateWriteBatchChannelWriter.WriteAsync(message);
        var assignedVectorClock = await replyChannel.Reader.ReadAsync();

        // The target actor replies with the vector clock timestamp associated with the
        // write operation. So we'll wait until this timestamp becomes visible in snapshots.
        await WaitForVectorClockVisibleInSnapshotsAsync(assignedVectorClock);
    }

    private async ValueTask WaitForVectorClockVisibleInSnapshotsAsync(VectorClock minimumClock)
    {
        var replyChannel = Channel.CreateBounded<Unit>(capacity: 1);

        var message = new AwaitClockMessage
        {
            MinimumClock = minimumClock,
            ReplyChannelWriter = replyChannel.Writer,
        };

        await _awaitClockChannelWriter.WriteAsync(message);
        await replyChannel.Reader.ReadAsync();
    }
}
