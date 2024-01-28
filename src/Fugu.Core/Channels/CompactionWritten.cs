using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct CompactionWritten(
    VectorClock Clock,
    Segment OutputSegment,
    ChangeSetCoordinates Changes
);
