using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly SemaphoreSlim _semaphore = new(1);

    private readonly IBackingStorage _storage;
    private readonly Channel<SegmentStatsUpdated> _segmentStatsUpdatedChannel;
    private readonly Channel<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannel;
    private readonly Channel<ChangesWritten> _changesWrittenChannel;
    private readonly Channel<SegmentsCompacted> _segmentsCompactedChannel;

    private readonly PriorityQueue<Segment, VectorClock> _segmentsAwaitingRemoval = new(
        Comparer<VectorClock>.Create((x, y) =>
        {
            var componentComparer = Comparer<long>.Default;
            var writeComparison = componentComparer.Compare(x.Write, y.Write);

            return writeComparison != 0
                ? writeComparison
                : componentComparer.Compare(x.Compaction, y.Compaction);
        }));

    public CompactionActor(
        IBackingStorage storage,
        Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel,
        Channel<OldestObservableSnapshotChanged> oldestObservableSnapshotChangedChannel,
        Channel<ChangesWritten> changesWrittenChannel,
        Channel<SegmentsCompacted> segmentsCompactedChannel)
    {
        _storage = storage;
        _segmentStatsUpdatedChannel = segmentStatsUpdatedChannel;
        _oldestObservableSnapshotChangedChannel = oldestObservableSnapshotChangedChannel;
        _changesWrittenChannel = changesWrittenChannel;
        _segmentsCompactedChannel = segmentsCompactedChannel;
    }

    public async Task RunAsync()
    {
        await Task.WhenAll(
            ProcessSegmentStatsUpdatedMessagesAsync(),
            ProcessOldestObservableSnapshotChangedMessagesAsync());

        _segmentsCompactedChannel.Writer.Complete();
    }

    private async Task ProcessSegmentStatsUpdatedMessagesAsync()
    {
        while (await _segmentStatsUpdatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _segmentStatsUpdatedChannel.Reader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                // Geometric series characterized by two parameters a and r:
                const double a = 100;       // Coefficient, also the size of slab #0
                const double r = 1.5;       // Common ratio, indicates by how much each added slab should be bigger than the last

                // Given the current number n of non-output segments in the store, calculate idealized capacity of the store as the
                // cumulative sum of an n-element (a, r) geometric series:
                var n = message.Stats.Count;
                var capacity = a * (1 - Math.Pow(r, n)) / (1 - r);

                // Calculate actual sum of "live" bytes:
                var totalLiveBytes = message.Stats.Sum(s => s.Value.LiveBytes);

                // We define utilization as the ratio of "live" bytes to idealized capacity given the current number of non-output
                // segments. If utilization drops too far below 1.0, this indicates that the store is using too many segments for
                // the amount of payload data it holds, and should be compacted to flush out stale data.
                var utilization = totalLiveBytes / capacity;

                // Setting the utilization threshold at 0.5 means that up to 50% of usable space within segments can be taken up
                // by stale data before a compaction is triggered. Choosing a higher threshold will allow less wasted space, at the
                // cost of higher write amplification. Choosing a lower threshold will reduce the frequency of compactions, but could
                // result in more space being wasted by stale data.
                if (utilization < 0.5)
                {
                    // We need to compact. Identify a suitable range of source segments.
                    // For each candidate range of segments, we are interested in two numbers:
                    // - By how much compacting these segments will reduce the idealized capacity; this is dependent only on n.
                    // - How much data we will likely need to copy during the compaction. Live bytes in source segments for sure;
                    //   potentially some "stale" bytes as well if they represent tombstones for values that may still exist in
                    //   previous segments.
                    // The ratio of both numbers yields the "efficiency" of compacting a specific candidate range, i.e., by how much
                    // each copied byte will be able to improve the utilization figure.

                    // TODO: Be smart about this, for now we always choose the first two segments as compaction inputs.

                    var sourceStats = message.Stats.Take(2).ToArray();
                    var minGeneration = sourceStats.First().Key.MinGeneration;
                    var maxGeneration = sourceStats.Last().Key.MaxGeneration;

                    var compactedClock = message.Clock with
                    {
                        Compaction = message.Clock.Compaction + 1,
                    };

                    // Prepare compaction segment
                    var outputSlab = await _storage.CreateSlabAsync();
                    var outputSegment = new Segment(minGeneration, maxGeneration, outputSlab);

                    CompactSegments(outputSlab, minGeneration, maxGeneration, sourceStats.Select(kvp => kvp.Key).ToArray());

                    // WriterActor will complete the "ChangesWritten" channel when the store shuts down,
                    // so we have to make sure it's still there before writing to it.
                    while (await _changesWrittenChannel.Writer.WaitToWriteAsync())
                    {
                        var succeeded = _changesWrittenChannel.Writer.TryWrite(
                            new ChangesWritten(
                                Clock: compactedClock,
                                Payloads: Array.Empty<KeyValuePair<byte[], SlabSubrange>>(),
                                Tombstones: new HashSet<byte[]>(),
                                OutputSegment: outputSegment
                            ));

                        if (succeeded)
                        {
                            break;
                        }
                    }

                    // Cannot delete the source segments right away because there might be active snapshots
                    // that reference it. Instead, add them to a list and delete them when SnapshotsActor signals
                    // that no states before the current compaction clock are observable in snapshots anymore.
                    _segmentsAwaitingRemoval.EnqueueRange(sourceStats.Select(kvp => kvp.Key), compactedClock);

                    // TODO: Tell allocation actor that the store's total capacity has decreased, so that it will
                    // account for it by making future segments smaller again.
                    // Note that we can either do this here, OR when the old segments actually get evicted because
                    // no snapshots reference them anymore.
                    //await _segmentsCompactedChannel.Writer.WriteAsync(
                    //    new SegmentsCompacted(
                    //        Clock: compactedClock,
                    //        CapacityChange: 0));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task ProcessOldestObservableSnapshotChangedMessagesAsync()
    {
        while (await _oldestObservableSnapshotChangedChannel.Reader.WaitToReadAsync())
        {
            var message = await _oldestObservableSnapshotChangedChannel.Reader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                while (_segmentsAwaitingRemoval.TryPeek(out var _, out var compactedAt) && message.Clock.Compaction >= compactedAt.Compaction)
                {
                    var segment = _segmentsAwaitingRemoval.Dequeue();

                    // TODO: Ask backing storage to remove it
                    //await _storage.RemoveSlabAsync(segment.Slab);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private void CompactSegments(IWritableSlab outputSlab, long minGeneration, long maxGeneration, Segment[] segments)
    {
        var pipeWriter = PipeWriter.Create(outputSlab.Output);
        var segmentWriter = new SegmentWriter(pipeWriter);

        // Header
        segmentWriter.WriteSegmentHeader(minGeneration, maxGeneration);
    }
}
