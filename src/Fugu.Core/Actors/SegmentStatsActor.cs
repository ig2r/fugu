using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SegmentStatsActor : Actor
{
    private readonly ChannelReader<UpdateSegmentStatsMessage> _updateSegmentStatsChannelReader;
    private readonly ChannelWriter<DummyMessage> _segmentStatsUpdatedChannelWriter;
    private readonly ChannelWriter<DummyMessage> _segmentEmptiedChannelWriter;

    public SegmentStatsActor(
        ChannelReader<UpdateSegmentStatsMessage> updateSegmentStatsChannelReader,
        ChannelWriter<DummyMessage> segmentStatsUpdatedChannelWriter,
        ChannelWriter<DummyMessage> segmentEmptiedChannelWriter)
    {
        _updateSegmentStatsChannelReader = updateSegmentStatsChannelReader;
        _segmentStatsUpdatedChannelWriter = segmentStatsUpdatedChannelWriter;
        _segmentEmptiedChannelWriter = segmentEmptiedChannelWriter;
    }

    public override Task RunAsync()
    {
        return HandleUpdateSegmentStatsMessagesAsync();
    }

    private async Task HandleUpdateSegmentStatsMessagesAsync()
    {
        while (await _updateSegmentStatsChannelReader.WaitToReadAsync())
        {
            if (_updateSegmentStatsChannelReader.TryRead(out var message))
            {
            }
        }

        // Input channel has completed, propagate completion
        _segmentStatsUpdatedChannelWriter.Complete();
        _segmentEmptiedChannelWriter.Complete();
    }
}
