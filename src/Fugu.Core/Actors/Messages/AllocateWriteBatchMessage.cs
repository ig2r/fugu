using Fugu.Core.Common;
using System.Threading.Channels;

namespace Fugu.Core.Actors.Messages;

public readonly record struct AllocateWriteBatchMessage(
    WriteBatch Batch,
    ChannelWriter<VectorClock> ReplyChannelWriter);
