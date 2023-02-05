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
}