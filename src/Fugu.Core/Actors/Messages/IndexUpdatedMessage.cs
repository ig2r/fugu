using Fugu.Core.Common;

namespace Fugu.Core.Actors.Messages;

public readonly record struct IndexUpdatedMessage(
    VectorClock Clock);