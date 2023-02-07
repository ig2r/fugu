using Fugu.Core.Common;

namespace Fugu.Core.Actors.Messages;

public readonly record struct UpdateIndexMessage(
    VectorClock Clock);