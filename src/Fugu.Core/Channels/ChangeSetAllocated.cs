using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Channels;

public readonly record struct ChangeSetAllocated(
    VectorClock Clock,
    ChangeSet ChangeSet,
    IWritableSlab OutputSlab
);
