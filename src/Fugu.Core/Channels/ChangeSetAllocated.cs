using Fugu.IO;
using Fugu.Utils;

namespace Fugu.Channels;

/// <summary>
/// Emitted by <see cref="Actors.AllocationActor"/> when a change set has been assigned to
/// go into a specific output segment.
/// </summary>
/// <param name="Clock">Logical clock value.</param>
/// <param name="ChangeSet">Changes to be persisted.</param>
/// <param name="OutputSlab">Output slab associated with the assigned output segment.</param>
public readonly record struct ChangeSetAllocated(
    VectorClock Clock,
    ChangeSet ChangeSet,
    IWritableSlab OutputSlab
);
