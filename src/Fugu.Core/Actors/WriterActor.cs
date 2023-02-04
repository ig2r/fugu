using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class WriterActor
{
    private readonly ChannelReader<DummyMessage> _writeWriteBatchChannelReader;
    private readonly ChannelWriter<DummyMessage> _updateIndexChannelWriter;

    public WriterActor(
        ChannelReader<DummyMessage> writeWriteBatchChannelReader,
        ChannelWriter<DummyMessage> updateIndexChannelWriter)
    {
        _writeWriteBatchChannelReader = writeWriteBatchChannelReader;
        _updateIndexChannelWriter = updateIndexChannelWriter;
    }
}
