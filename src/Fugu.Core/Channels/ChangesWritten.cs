using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct WrittenPayload(
    byte[] Key,
    long ValueOffset,
    int ValueLength
);

public readonly record struct ChangesWritten(
    VectorClock Clock,
    Segment OutputSegment,
    IReadOnlyList<WrittenPayload> Payloads,
    IReadOnlySet<byte[]> Tombstones
);
