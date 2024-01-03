using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Core.Tests;

public class SegmentStatsTrackerTests
{
    [Fact]
    public async Task OnPayloadAdded_PayloadInUntrackedSegment_TrackedAsLiveBytes()
    {
        var tracker = new SegmentStatsTracker();

        var storage = new InMemoryStorage();
        var slab = await storage.CreateSlabAsync();
        var segment = new Segment(1, 1, slab);
        var payload = new KeyValuePair<byte[], SlabSubrange>("foo"u8.ToArray(), new SlabSubrange(10, 23));

        tracker.OnPayloadAdded(segment, payload);

        var statsBySegment = tracker.ToImmutable();

        // There should now be a single entry in the tracker
        var singleSegmentStats = Assert.Single(statsBySegment).Value;
        
        // The entire combined size of key + value for the payload should count against live bytes
        Assert.Equal(0, singleSegmentStats.StaleBytes);
        Assert.Equal(3 + 23, singleSegmentStats.LiveBytes);
        Assert.Equal(3 + 23, singleSegmentStats.TotalBytes);
    }
}
