namespace Fugu.Utils;

public readonly record struct VectorClock(long Write, long Compaction)
{
    public static bool operator >(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write > rhs.Write
            && lhs.Compaction > rhs.Compaction;
    }
     
    public static bool operator <(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write < rhs.Write
            && lhs.Compaction < rhs.Compaction;
    }

    public static bool operator >=(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write >= rhs.Write
            && lhs.Compaction >= rhs.Compaction;
    }

    public static bool operator <=(VectorClock lhs, VectorClock rhs)
    {
        return lhs.Write <= rhs.Write
            && lhs.Compaction <= rhs.Compaction;
    }
}
