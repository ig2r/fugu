using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct IndexUpdated(
    VectorClock Clock
);
