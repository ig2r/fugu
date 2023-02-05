using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class WriterActor : Actor
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

    public override Task RunAsync()
    {
        return HandleWriteWriteBatchMessagesAsync();
    }

    private async Task HandleWriteWriteBatchMessagesAsync()
    {
        while (await _writeWriteBatchChannelReader.WaitToReadAsync())
        {
            if (_writeWriteBatchChannelReader.TryRead(out var message))
            {

            }
        }
    }
}
