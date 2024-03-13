namespace Fugu.Utils;

public readonly record struct StoreStats(
    IReadOnlyList<Segment> Keys,
    IReadOnlyList<SegmentStats> Stats);
