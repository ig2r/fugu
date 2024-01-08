using Fugu.Utils;

namespace Fugu.Channels;

/// <summary>
/// Emitted by <see cref="Actors.CompactionActor"/> when two or more segments have been compacted to
/// re-balance the store, resulting in a reduction in idealized store capacity.
/// </summary>
/// <param name="Clock">Logical clock value.</param>
/// <param name="CapacityChange">Net change to idealized store capacity; typically negative.</param>
public readonly record struct SegmentsCompacted(
    VectorClock Clock,
    long CapacityChange
);
