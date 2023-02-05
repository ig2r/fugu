using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class IndexActor : Actor
{
    private readonly ChannelReader<DummyMessage> _updateIndexChannelReader;
    private readonly ChannelWriter<DummyMessage> _indexUpdatedChannelWriter;
    private readonly ChannelWriter<DummyMessage> _updateSegmentStatsChannelWriter;

    public IndexActor(
        ChannelReader<DummyMessage> updateIndexChannelReader,
        ChannelWriter<DummyMessage> indexUpdatedChannelWriter,
        ChannelWriter<DummyMessage> updateSegmentStatsChannelWriter)
    {
        _updateIndexChannelReader = updateIndexChannelReader;
        _indexUpdatedChannelWriter = indexUpdatedChannelWriter;
        _updateSegmentStatsChannelWriter = updateSegmentStatsChannelWriter;
    }

    public override Task RunAsync()
    {
        return HandleUpdateIndexMessagesAsync();
    }

    private async Task HandleUpdateIndexMessagesAsync()
    {
        while (await _updateIndexChannelReader.WaitToReadAsync())
        {
            if (_updateIndexChannelReader.TryRead(out var message))
            {

            }
        }

        // Input channel has completed, propagate completion
        _indexUpdatedChannelWriter.Complete();
        _updateSegmentStatsChannelWriter.Complete();
    }
}
