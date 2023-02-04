using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class AllocationActor
{
    private readonly ChannelReader<DummyMessage> _allocateWriteBatchChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEvictedChannelReader;
    private readonly ChannelWriter<DummyMessage> _writeWriteBatchChannelWriter;

    public AllocationActor(
        ChannelReader<DummyMessage> allocateWriteBatchChannelReader,
        ChannelReader<DummyMessage> segmentEvictedChannelReader,
        ChannelWriter<DummyMessage> writeWriteBatchChannelWriter)
    {
        _allocateWriteBatchChannelReader = allocateWriteBatchChannelReader;
        _segmentEvictedChannelReader = segmentEvictedChannelReader;
        _writeWriteBatchChannelWriter = writeWriteBatchChannelWriter;
    }
}
