using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct OldestObservableSnapshotChanged(
    VectorClock Clock
);
