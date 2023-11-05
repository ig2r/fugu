using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct ChangesWritten(
    VectorClock Clock,
    Segment OutputSegment,
    IReadOnlySet<byte[]> Tombstones
);
