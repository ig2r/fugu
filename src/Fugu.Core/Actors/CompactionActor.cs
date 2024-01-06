using Fugu.Channels;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly Channel<SegmentStatsUpdated> _segmentStatsUpdatedChannel;

    public CompactionActor(Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel)
    {
        _segmentStatsUpdatedChannel = segmentStatsUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _segmentStatsUpdatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _segmentStatsUpdatedChannel.Reader.ReadAsync();

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
                // TODO: identify a suitable range of segments for compaction
            }
        }
    }
}
