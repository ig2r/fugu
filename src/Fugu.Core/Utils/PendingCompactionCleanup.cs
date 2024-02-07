namespace Fugu.Utils;

/// <summary>
/// Remaining cleanup activities to be performed after a compaction.
/// </summary>
/// <param name="CompactionClock">The compaction clock component after which the cleanup can be performed.</param>
/// <param name="Segments">The source segments for the compaction, to be removed from the store.</param>
/// <param name="CapacityChange">The net change in total store capacity that resulted from the compaction.</param>
internal record PendingCompactionCleanup(
    long CompactionClock,
    Segment[] Segments,
    long CapacityChange
);
