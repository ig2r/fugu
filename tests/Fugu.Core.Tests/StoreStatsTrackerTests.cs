using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Core.Tests;

public class StoreStatsTrackerTests
{
    [Fact]
    public async Task Add_SegmentWithSinglePayload_ReflectsPayloadInStats()
    {
        // Arrange tracker and some moving bits we need to make it track a (fictional) payload
        var tracker = new StoreStatsTracker();

        var storage = new InMemoryStorage();
        var slab = await storage.CreateSlabAsync();
        var segment = new Segment(1, 1, slab);
        var builder = new SegmentStatsBuilder(segment);

        var payload = new KeyValuePair<byte[], SlabSubrange>("foo"u8.ToArray(), new SlabSubrange(10, 23));
        builder.OnPayloadAdded(payload);

        tracker.Add(builder);

        var stats = tracker.ToImmutable();
        var singleKey = Assert.Single(stats.Keys);
        var singleStat = Assert.Single(stats.Stats);

        Assert.Same(segment, singleKey);
        Assert.Equal(payload.Key.Length + payload.Value.Length, singleStat.LiveBytes);
        Assert.Equal(0, singleStat.StaleBytes);
    }
}
