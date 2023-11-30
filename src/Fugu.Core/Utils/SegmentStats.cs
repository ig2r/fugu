namespace Fugu.Utils;

/// <summary>
/// Tracks compaction-related data for a single segment.
/// </summary>
/// <param name="TotalBytes">The total number of key/value bytes present in the associated segment.</param>
/// <param name="StaleBytes">The number of "stale" key/value bytes in the associated segment.</param>
public readonly record struct SegmentStats(long TotalBytes, long StaleBytes)
{
    public long LiveBytes => TotalBytes - StaleBytes;
}
