namespace Fugu.Core.Common;

public readonly record struct SegmentStats(
    long LiveBytes,
    long DeadBytes)
{
    public long TotalBytes => LiveBytes + DeadBytes;
}
