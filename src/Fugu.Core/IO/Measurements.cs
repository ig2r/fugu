using Fugu.Core.IO.Format;
using System.Runtime.CompilerServices;

namespace Fugu.Core.IO;

public static class Measurements
{
    // Number of bytes that will be used in each segment for metadata and integrity
    public static readonly int SegmentOverheadSize =
        Unsafe.SizeOf<SegmentHeader>() + Unsafe.SizeOf<SegmentTrailer>();

    // Determines the total number of bytes that a given WriteBatch will occupy when written to the backing table set
    public static long Measure(WriteBatch batch)
    {
        long size = Unsafe.SizeOf<CommitHeader>() + Unsafe.SizeOf<CommitTrailer>();
        size += batch.PendingPuts.Sum((kvp) => (long)(Unsafe.SizeOf<CommitKeyValue>() + kvp.Key.Length + kvp.Value.Length));
        size += batch.PendingRemovals.Sum(key => (long)key.Length);
        return size;
    }
}
