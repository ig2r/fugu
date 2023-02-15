using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SegmentStatsActor : Actor
{
    private readonly ChannelReader<UpdateSegmentStatsMessage> _updateSegmentStatsChannelReader;
    private readonly ChannelWriter<DummyMessage> _segmentStatsUpdatedChannelWriter;
    private readonly ChannelWriter<DummyMessage> _segmentEmptiedChannelWriter;

    private ImmutableDictionary<Segment, SegmentStats> _segmentStats =
        ImmutableDictionary<Segment, SegmentStats>.Empty;

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
                var builder = _segmentStats.ToBuilder();

                foreach (var (segment, change) in message.StatsChanges)
                {
                    var stats = builder.GetValueOrDefault(segment);

                    builder[segment] = stats with
                    {
                        LiveBytes = stats.LiveBytes + change.LiveBytesWritten - change.BytesDisplaced,
                        DeadBytes = stats.DeadBytes + change.BytesDisplaced,
                    };
                }

                _segmentStats = builder.ToImmutable();
            }
        }

        // Input channel has completed, propagate completion
        _segmentStatsUpdatedChannelWriter.Complete();
        _segmentEmptiedChannelWriter.Complete();
    }
}
