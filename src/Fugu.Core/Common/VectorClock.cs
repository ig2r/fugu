namespace Fugu.Core.Common;
public readonly record struct VectorClock(
    long Write,
    long Compaction);