using Fugu.Utils;

namespace Fugu.Core.Tests;

public class BalancingStrategyTests
{
    private readonly BalancingStrategy _balancingStrategy = new(120, 1.5);

    [Fact]
    public void GetOutputSizeLimit_EmptyStore_ReturnsCoefficientAsSizeOfFirstSegment()
    {
        var limit = _balancingStrategy.GetOutputSizeLimit(totalBytes: 0);
        Assert.Equal(120, limit);
    }

    [Fact]
    public void GetOutputSizeLimit_StoreWithIdealFirstSegment_SizesSecondSegmentUsingGrowthFactor()
    {
        var limit = _balancingStrategy.GetOutputSizeLimit(totalBytes: 120);
        Assert.Equal(120 * 1.5, limit);
    }

    [Fact]
    public void GetIdealizedCapacity_EmptyStore_ReturnsZero()
    {
        // A store with no segments at all trivially has 0 capacity.
        var capacity = _balancingStrategy.GetIdealizedCapacity(segmentCount: 0);
        Assert.Equal(0, capacity);
    }

    [Fact]
    public void GetIdealizedCapacity_ThreeSegments_ReturnsCapacityBasedOnGeometricGrowth()
    {
        var capacity = _balancingStrategy.GetIdealizedCapacity(segmentCount: 3);
        Assert.Equal(120 + 1.5 * 120 + 1.5 * 1.5 * 120, capacity);
    }
}
