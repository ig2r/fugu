namespace Fugu;

/// <summary>
/// Tracks compaction-related metrics for a single segment.
/// </summary>
/// <param name="TotalValueCount">Overall number of value items in this segment.</param>
/// <param name="DeadValueCount">Number of value items in this segment that have been deleted or displaced by a newer value for the same key.</param>
/// <param name="TotalTombstoneCount">Overall number of tombstones in this segment.</param>
/// <param name="DeadTombstoneCount">Number of dead tombstones in this segment, e.g., whose key was not in the index when the tombstone was written.</param>
internal readonly record struct SegmentMetrics(
    int TotalValueCount,
    // TODO: Can differentiate deleted (= tombstoned) values from displaced (= superceded by newer) values
    int DeadValueCount,
    int TotalTombstoneCount,
    int DeadTombstoneCount
);
