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

    private readonly Dictionary<Segment, SegmentStats> _segmentStats = new();

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
            var indexBuilder = _index.ToBuilder();

            // Process incoming payloads
            foreach (var payload in message.Payloads)
            {
                // If this payload replaces an existing payload with the same key, mark the previous payload as stale
                if (indexBuilder.TryGetValue(payload.Key, out var previousIndexEntry))
                {
                    MarkLiveBytesAsStale(previousIndexEntry.Segment, payload.Key.Length + previousIndexEntry.Subrange.Length);
                }

                indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
                TrackAddedLiveBytes(message.OutputSegment, payload.Key.Length + payload.Value.Length);
            }
            
            // Process incoming tombstones. Tombstones will only ever increase the amount of "stale" bytes in the store.
            foreach (var tombstone in message.Tombstones)
            {
                if (indexBuilder.TryGetValue(tombstone, out var previousIndexEntry))
                {
                    MarkLiveBytesAsStale(previousIndexEntry.Segment, tombstone.Length + previousIndexEntry.Subrange.Length);
                }

                indexBuilder.Remove(tombstone);
                TrackAddedStaleBytes(message.OutputSegment, tombstone.Length);
            }

            _index = indexBuilder.ToImmutable();

            await _indexUpdatedChannel.Writer.WriteAsync(
                new IndexUpdated(
                    Clock: message.Clock,
                    Index: _index));
        }

        // Propagate completion
        _indexUpdatedChannel.Writer.Complete();
    }

    private void TrackAddedLiveBytes(Segment segment, int byteCount)
    {
        var stats = _segmentStats.TryGetValue(segment, out var s) ? s : new();

        _segmentStats[segment] = stats with
        {
            TotalBytes = stats.TotalBytes + byteCount,
        };
    }

    private void TrackAddedStaleBytes(Segment segment, int byteCount)
    {
        var stats = _segmentStats.TryGetValue(segment, out var s) ? s : new();

        _segmentStats[segment] = stats with
        {
            TotalBytes = stats.TotalBytes + byteCount,
            StaleBytes = stats.StaleBytes + byteCount,
        };
    }

    private void MarkLiveBytesAsStale(Segment segment, int byteCount)
    {
        var stats = _segmentStats[segment];

        _segmentStats[segment] = stats with
        {
            StaleBytes = stats.StaleBytes + byteCount,
        };
    }
}
