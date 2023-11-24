using Fugu.Channels;
using Fugu.Utils;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class IndexActor
{
    private readonly Channel<ChangesWritten> _changesWrittenChannel;
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;

    private ImmutableDictionary<byte[], IndexEntry> _index = ImmutableDictionary.Create<byte[], IndexEntry>(ByteArrayEqualityComparer.Shared);

    public IndexActor(Channel<ChangesWritten> changesWrittenChannel, Channel<IndexUpdated> indexUpdatedChannel)
    {
        _changesWrittenChannel = changesWrittenChannel;
        _indexUpdatedChannel = indexUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _changesWrittenChannel.Reader.WaitToReadAsync())
        {
            var message = await _changesWrittenChannel.Reader.ReadAsync();

            var builder = _index.ToBuilder();

            foreach (var payload in message.Payloads)
            {
                builder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
            }
            
            foreach (var tombstone in message.Tombstones)
            {
                builder.Remove(tombstone);
            }

            _index = builder.ToImmutable();

            await _indexUpdatedChannel.Writer.WriteAsync(
                new IndexUpdated(
                    Clock: message.Clock,
                    Index: _index));
        }

        // Propagate completion
        _indexUpdatedChannel.Writer.Complete();
    }
}
