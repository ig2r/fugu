using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Core.Tests;

public class SegmentStatsTrackerTests
{
    [Fact]
    public async Task Add_SegmentWithSinglePayload_ReflectsPayloadInStats()
    {
        // Arrange tracker and some moving bits we need to make it track a (fictional) payload
        var tracker = new SegmentStatsTracker();

        var storage = new InMemoryStorage();
        var slab = await storage.CreateSlabAsync();
        var segment = new Segment(1, 1, slab);
        var builder = new SegmentStatsBuilder(segment);

        var payload = new KeyValuePair<byte[], SlabSubrange>("foo"u8.ToArray(), new SlabSubrange(10, 23));
        builder.OnPayloadAdded(payload);

        tracker.Add(builder);

        var stats = tracker.ToImmutable();
        var singleStatsItem = Assert.Single(stats);

        Assert.Same(segment, singleStatsItem.Key);
        Assert.Equal(payload.Key.Length + payload.Value.Length, singleStatsItem.Value.LiveBytes);
        Assert.Equal(0, singleStatsItem.Value.StaleBytes);
    }
}
