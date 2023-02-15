using Fugu.Core.Common;

namespace Fugu.Core.Actors.Messages;

public readonly record struct SegmentStatsUpdatedMessage(
    VectorClock Clock,
    IReadOnlyDictionary<Segment, SegmentStats> SegmentStats);
