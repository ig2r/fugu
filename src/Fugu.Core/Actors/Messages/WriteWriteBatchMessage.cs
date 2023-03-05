using Fugu.Core.Common;
using Fugu.Core.IO;

namespace Fugu.Core.Actors.Messages;

public readonly record struct WriteWriteBatchMessage(
    WriteBatch Batch,
    VectorClock Clock,
    WritableTable OutputTable);