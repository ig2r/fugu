using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct ChangesWritten(
    VectorClock Clock,
    Segment OutputSegment
);
