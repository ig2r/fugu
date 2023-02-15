namespace Fugu.Core.Common;

public readonly record struct VectorClock(
    long Write,
    long Compaction)
{
    public static bool operator >=(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write >= rhs.Write && lhs.Compaction >= rhs.Compaction;
    }

    public static bool operator <=(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write <= rhs.Write && lhs.Compaction <= lhs.Compaction;
    }

    public static VectorClock Max(VectorClock x, VectorClock y)
    {
        return new VectorClock(Math.Max(x.Write, y.Write), Math.Max(x.Compaction, y.Compaction));
    }
}