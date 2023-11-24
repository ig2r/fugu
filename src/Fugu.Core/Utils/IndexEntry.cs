namespace Fugu.Utils;

public readonly record struct IndexEntry(
    Segment Segment,
    SlabSubrange Subrange
);
