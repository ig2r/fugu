using Fugu.Core.Common;
using Fugu.Core.IO;

namespace Fugu.Core.Actors.Messages;

public readonly record struct UpdateIndexMessage(
    VectorClock Clock,
    Segment Segment,
    IReadOnlyDictionary<Key, PayloadLocator> Payloads,
    IReadOnlySet<Key> Removals);