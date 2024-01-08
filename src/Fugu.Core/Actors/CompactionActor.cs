using Fugu.Channels;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly Channel<SegmentStatsUpdated> _segmentStatsUpdatedChannel;
    private readonly Channel<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannel;
    private readonly Channel<ChangesWritten> _changesWrittenChannel;
    private readonly Channel<SegmentsCompacted> _segmentsCompactedChannel;

    public CompactionActor(
        Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel,
        Channel<OldestObservableSnapshotChanged> oldestObservableSnapshotChangedChannel,
        Channel<ChangesWritten> changesWrittenChannel,
        Channel<SegmentsCompacted> segmentsCompactedChannel)
    {
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

            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}
