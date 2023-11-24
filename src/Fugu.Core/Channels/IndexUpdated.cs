using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct IndexUpdated(
    VectorClock Clock,
    IReadOnlyDictionary<byte[], IndexEntry> Index
);
