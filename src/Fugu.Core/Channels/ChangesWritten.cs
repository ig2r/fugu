using Fugu.Utils;

namespace Fugu.Channels;

/// <summary>
/// Emitted by <see cref="Actors.WriterActor"/> when a change set has been written to its assigned
/// output segment.
/// </summary>
/// <param name="Clock">Logical clock value.</param>
/// <param name="OutputSegment">Output segment.</param>
/// <param name="Payloads">Payloads written to the output segment, including value offsets.</param>
/// <param name="Tombstones">Tombstones written to the output segment.</param>
public readonly record struct ChangesWritten(
    VectorClock Clock,
    Segment OutputSegment,
    IReadOnlyList<KeyValuePair<byte[], SlabSubrange>> Payloads,
    IReadOnlySet<byte[]> Tombstones
);
