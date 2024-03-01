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
            new TestSegmentMetadata(1, 3),          // This one should be ignored, since the other two cover the entire 1-3 range.
            new TestSegmentMetadata(1, 2),
        ];

        var result = BootstrapSegmentOrderer.GetBootstrapOrder(segments);

        Assert.Equal([segments[2], segments[0]], result);
    }

    private record TestSegmentMetadata(long MinGeneration, long MaxGeneration) : ISegmentMetadata;
}
