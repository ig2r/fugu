namespace Fugu.Core.Actors.Messages;

public readonly record struct SegmentStatsChange(
    long LiveBytesWritten,
    long BytesDisplaced);
