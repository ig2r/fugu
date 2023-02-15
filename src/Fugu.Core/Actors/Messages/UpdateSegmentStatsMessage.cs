using Fugu.Core.Common;

namespace Fugu.Core.Actors.Messages;

public readonly record struct UpdateSegmentStatsMessage(
    VectorClock Clock,
    IReadOnlyDictionary<Segment, SegmentStatsChange> StatsChanges);