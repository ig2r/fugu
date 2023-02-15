using Fugu.Core.Common;

namespace Fugu.Core.Tests;

public class VectorClockTests
{
    [Fact]
    public void GreaterEqual_BothComponentsStrictlyGreaterEqual_ReturnsTrue()
    {
        var lhs = new VectorClock(2, 2);
        var rhs = new VectorClock(1, 1);

        Assert.True(lhs >= rhs);
    }

    [Fact]
    public void GreaterEqual_OneComponentLessThanRhs_ReturnsFalse()
    {
        var lhs = new VectorClock(2, 1);
        var rhs = new VectorClock(1, 2);

        Assert.False(lhs >= rhs);
    }

    [Fact]
    public void Max_ReturnsComponentWiseMaximum()
    {
        var x = new VectorClock(1, 4);
        var y = new VectorClock(2, 3);

        var max = VectorClock.Max(x, y);

        Assert.Equal(2, max.Write);
        Assert.Equal(4, max.Compaction);
    }
}