using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class IndexActor : Actor
{
    private readonly ChannelReader<UpdateIndexMessage> _updateIndexChannelReader;
    private readonly ChannelWriter<IndexUpdatedMessage> _indexUpdatedChannelWriter;
    private readonly ChannelWriter<UpdateSegmentStatsMessage> _updateSegmentStatsChannelWriter;

    public IndexActor(
        ChannelReader<UpdateIndexMessage> updateIndexChannelReader,
        ChannelWriter<IndexUpdatedMessage> indexUpdatedChannelWriter,
        ChannelWriter<UpdateSegmentStatsMessage> updateSegmentStatsChannelWriter)
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
                // TODO: Update index

                // Tell downstream actors about this
                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdatedMessage
                    {
                        Clock = message.Clock,
                    });

                await _updateSegmentStatsChannelWriter.WriteAsync(
                    new UpdateSegmentStatsMessage
                    {
                        Clock = message.Clock,
                    });
            }
        }

        // Input channel has completed, propagate completion
        _indexUpdatedChannelWriter.Complete();
        _updateSegmentStatsChannelWriter.Complete();
    }
}
