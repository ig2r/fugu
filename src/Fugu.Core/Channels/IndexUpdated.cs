using Fugu.Utils;

namespace Fugu.Channels;

/// <summary>
/// Emitted by <see cref="Actors.IndexActor"/> when the index has been updated to reflect changes
/// written to the store.
/// </summary>
/// <param name="Clock">Logical clock value.</param>
/// <param name="Index">State of the index at the given clock value.</param>
public readonly record struct IndexUpdated(
    VectorClock Clock,
    IReadOnlyDictionary<byte[], IndexEntry> Index
);
