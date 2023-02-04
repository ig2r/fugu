using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SegmentStatsActor
{
    private readonly ChannelReader<DummyMessage> _updateSegmentStatsChannelReader;
    private readonly ChannelWriter<DummyMessage> _segmentStatsUpdatedChannelWriter;
    private readonly ChannelWriter<DummyMessage> _segmentEmptiedChannelWriter;

    public SegmentStatsActor(
        ChannelReader<DummyMessage> updateSegmentStatsChannelReader,
        ChannelWriter<DummyMessage> segmentStatsUpdatedChannelWriter,
        ChannelWriter<DummyMessage> segmentEmptiedChannelWriter)
    {
        _updateSegmentStatsChannelReader = updateSegmentStatsChannelReader;
        _segmentStatsUpdatedChannelWriter = segmentStatsUpdatedChannelWriter;
        _segmentEmptiedChannelWriter = segmentEmptiedChannelWriter;
    }
}
