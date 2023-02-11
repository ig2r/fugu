using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using Fugu.Core.IO;
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
                var builder = _index.ToBuilder();

                // Update index
                foreach (var (key, payloadLocator) in message.Payloads)
                {
                    var indexEntry = new IndexEntry(message.Segment, payloadLocator);

                    if (builder.TryGetValue(key, out var previousIndexEntry))
                    {
                        // TODO: Handle previous value being displaced
                    }

                    builder[key] = indexEntry;
                }

                foreach (var key in message.Removals)
                {
                    if (builder.Remove(key, out var previousIndexEntry))
                    {
                        // TODO: Handle a value having been removed
                    }
                    else
                    {
                        // Removal was a dud
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
                    });
            }
        }

        // Input channel has completed, propagate completion
        _indexUpdatedChannelWriter.Complete();
        _updateSegmentStatsChannelWriter.Complete();
    }
}
