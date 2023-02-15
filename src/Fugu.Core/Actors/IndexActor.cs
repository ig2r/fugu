using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class IndexActor : Actor
{
    private readonly ChannelReader<UpdateIndexMessage> _updateIndexChannelReader;
    private readonly ChannelWriter<IndexUpdatedMessage> _indexUpdatedChannelWriter;
    private readonly ChannelWriter<UpdateSegmentStatsMessage> _updateSegmentStatsChannelWriter;

    private Index _index = ImmutableDictionary.Create<Key, IndexEntry>(keyComparer: new ByteKeyEqualityComparer());

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
                // We'll use this to track stats changes
                var statsChanges = new Dictionary<Segment, SegmentStatsChange>();

                var builder = _index.ToBuilder();

                // Update index: handle writes
                foreach (var (key, payloadLocator) in message.Payloads)
                {
                    var indexEntry = new IndexEntry(message.Segment, payloadLocator);

                    if (builder.TryGetValue(key, out var previousIndexEntry))
                    {
                        // Displacing an existing key in the index
                        var prevStats = statsChanges.GetValueOrDefault(previousIndexEntry.Segment);
                        prevStats = prevStats with { BytesDisplaced = prevStats.BytesDisplaced + key.Length + previousIndexEntry.PayloadLocator.Size };
                        statsChanges[previousIndexEntry.Segment] = prevStats;
                    }

                    var stats = statsChanges.GetValueOrDefault(message.Segment);
                    stats = stats with { LiveBytesWritten = stats.LiveBytesWritten + key.Length + payloadLocator.Size };
                    statsChanges[message.Segment] = stats;

                    builder[key] = indexEntry;
                }

                // Update index: handle removals
                foreach (var key in message.Removals)
                {
                    if (builder.Remove(key, out var previousIndexEntry))
                    {
                        // Displacing an existing key in the index
                        var prevStats = statsChanges.GetValueOrDefault(previousIndexEntry.Segment);
                        prevStats = prevStats with { BytesDisplaced = prevStats.BytesDisplaced + key.Length + previousIndexEntry.PayloadLocator.Size };
                        statsChanges[previousIndexEntry.Segment] = prevStats;

                        var stats = statsChanges.GetValueOrDefault(message.Segment);
                        stats = stats with { LiveBytesWritten = stats.LiveBytesWritten + key.Length };
                        statsChanges[message.Segment] = stats;
                    }
                    else
                    {
                        // Removal attempt was a dud, there was nothing in the index
                        var stats = statsChanges.GetValueOrDefault(message.Segment);
                        stats = stats with
                        {
                            LiveBytesWritten = stats.LiveBytesWritten + key.Length,
                            BytesDisplaced = stats.BytesDisplaced + key.Length,
                        };
                        statsChanges[message.Segment] = stats;
                    }
                }

                _index = builder.ToImmutable();

                // Tell downstream actors about this
                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdatedMessage
                    {
                        Clock = message.Clock,
                        Index = _index,
                    });

                await _updateSegmentStatsChannelWriter.WriteAsync(
                    new UpdateSegmentStatsMessage
                    {
                        Clock = message.Clock,
                        StatsChanges = statsChanges,
                    });
            }
        }

        // Input channel has completed, propagate completion
        _indexUpdatedChannelWriter.Complete();
        _updateSegmentStatsChannelWriter.Complete();
    }
}
