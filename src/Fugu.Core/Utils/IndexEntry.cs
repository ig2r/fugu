namespace Fugu.Utils;

public readonly record struct IndexEntry(
    Segment Segment,
    long Offset,
    int Length
);
