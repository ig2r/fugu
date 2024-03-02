using Fugu.Utils;

namespace Fugu.Core.Tests;

public class BootstrapSegmentOrdererTests
{
    [Fact]
    public void GetBootstrapOrder_EmptySegmentsList_ReturnsEmpty()
    {
        var result = BootstrapSegmentOrderer.GetBootstrapOrder([]);
        Assert.Empty(result);
    }

    [Fact]
    public void GetBootstrapOrder_NonOverlappingContiguousRanges_LoadsInGenerationOrder()
    {
        ISegmentMetadata[] segments =
        [
            new TestSegmentMetadata(2, 5),
            new TestSegmentMetadata(1, 1),
            new TestSegmentMetadata(6, 7),
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[1], segments[0], segments[2]], result);
    }

    [Fact]
    public void GetBootstrapOrder_EquivalentRanges_PrefersSmallerSegments()
    {
        ISegmentMetadata[] segments =
        [
            new TestSegmentMetadata(3, 3),
            new TestSegmentMetadata(1, 3),      // This one should be ignored, since the other two cover the entire 1-3 range.
            new TestSegmentMetadata(1, 2),
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[2], segments[0]], result);
    }

    [Fact]
    public void GetBootstrapOrder_MissingGen1InSmallSegments_CoversWithLargerSegment()
    {
        // The following set of segments has (2, 2) and (3, 3) ranges, but does NOT have a (1, 1) range. This could have
        // happened if the host program crashed after a compaction of (1, 1) through (3, 3) into (1, 3), at a point where
        // (1, 1) had already been removed.
        ISegmentMetadata[] segments =
        [
            new TestSegmentMetadata(2, 2),
            new TestSegmentMetadata(3, 3),
            new TestSegmentMetadata(4, 4),
            new TestSegmentMetadata(1, 3),      // This one should be used instead of the (2, 2) and (3, 3) segments.
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[3], segments[2]], result);
    }

    [Fact]
    public void GetBootstrapOrder_MissingLastGenInSmallSegments_CoversWithLargerSegment()
    {
        // The following set of segments could have occurred after compacting segments (1, 1), (2, 2) and (3, 3) into a
        // new (1, 3) segment, but the host program crashed after the (3, 3) segment had already been removed and (1, 1)
        // and (2, 2) had not.
        ISegmentMetadata[] segments =
        [
            new TestSegmentMetadata(1, 1),
            new TestSegmentMetadata(1, 3),      // Should use this one only.
            new TestSegmentMetadata(2, 2),
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[1]], result);
    }

    [Fact]
    public void GetBootstrapOrder_GapInSmallSegments_CoversWithLargerSegment()
    {
        ISegmentMetadata[] segments =
        [
            new TestSegmentMetadata(1, 1),
            new TestSegmentMetadata(3, 3),
            new TestSegmentMetadata(4, 4),
            new TestSegmentMetadata(1, 3),      // This one should be used instead of the (1, 1) and (3, 3) segments.
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[3], segments[2]], result);
    }

    private record TestSegmentMetadata(long MinGeneration, long MaxGeneration) : ISegmentMetadata;
}
