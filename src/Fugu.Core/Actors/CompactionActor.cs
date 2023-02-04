using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class CompactionActor
{
    private readonly ChannelReader<DummyMessage> _segmentStatsUpdatedChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEmptiedChannelReader;
    private readonly ChannelReader<DummyMessage> _snapshotsUpdatedChannelReader;
    private readonly ChannelWriter<DummyMessage> _updateIndexChannelWriter;
    private readonly ChannelWriter<DummyMessage> _writersegmentEvictedChannelWriter;

    public CompactionActor(
        ChannelReader<DummyMessage> segmentStatsUpdatedChannelReader,
        ChannelReader<DummyMessage> segmentEmptiedChannelReader,
        ChannelReader<DummyMessage> snapshotsUpdatedChannelReader,
        ChannelWriter<DummyMessage> updateIndexChannelWriter,
        ChannelWriter<DummyMessage> writersegmentEvictedChannelWriter)
    {
        _segmentStatsUpdatedChannelReader = segmentStatsUpdatedChannelReader;
        _segmentEmptiedChannelReader = segmentEmptiedChannelReader;
        _snapshotsUpdatedChannelReader = snapshotsUpdatedChannelReader;
        _updateIndexChannelWriter = updateIndexChannelWriter;
        _writersegmentEvictedChannelWriter = writersegmentEvictedChannelWriter;
    }
}
