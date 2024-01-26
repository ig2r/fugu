using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Core.Tests;

public class SegmentStatsTrackerTests
{
    //[Fact]
    //public async Task OnPayloadAdded_DoesNotImmediatelyReflectInStats()
    //{
    //    var tracker = new SegmentStatsTracker();

    //    var storage = new InMemoryStorage();
    //    var slab = await storage.CreateSlabAsync();
    //    var segment = new Segment(1, 1, slab);
    //    var payload = new KeyValuePair<byte[], SlabSubrange>("foo"u8.ToArray(), new SlabSubrange(10, 23));

    //    tracker.OnPayloadAdded(segment, payload);

    //    var statsBySegment = tracker.ToImmutable();

    //    Assert.Empty(statsBySegment);
    //}

    //[Fact]
    //public async Task OnPayloadAdded_SwitchingToNewOutputSegment_PreviousOutputSegmentReflectsInStats()
    //{
    //    var tracker = new SegmentStatsTracker();

    //    var storage = new InMemoryStorage();
    //    var slab = await storage.CreateSlabAsync();
    //    var segment1 = new Segment(1, 1, slab);
    //    var payload1 = new KeyValuePair<byte[], SlabSubrange>("foo"u8.ToArray(), new SlabSubrange(10, 23));
    //    var segment2 = new Segment(2, 2, slab);
    //    var payload2 = new KeyValuePair<byte[], SlabSubrange>("bar"u8.ToArray(), new SlabSubrange(10, 42));

    //    tracker.OnPayloadAdded(segment1, payload1);
    //    tracker.OnPayloadAdded(segment2, payload2);

    //    var statsBySegment = tracker.ToImmutable();

    //    // There should now be a single entry in the tracker for the previous output segment
    //    var singleSegmentStats = Assert.Single(statsBySegment);
    //    Assert.Equal(segment1, singleSegmentStats.Key);

    //    // The entire combined size of key + value for the payload should count against live bytes
    //    Assert.Equal(0, singleSegmentStats.Value.StaleBytes);
    //    Assert.Equal(3 + 23, singleSegmentStats.Value.LiveBytes);
    //    Assert.Equal(3 + 23, singleSegmentStats.Value.TotalBytes);
    //}
}
