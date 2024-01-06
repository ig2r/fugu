﻿using Fugu.Channels;
using Fugu.Utils;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed partial class IndexActor
{
    private readonly Channel<ChangesWritten> _changesWrittenChannel;
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;
    private readonly Channel<SegmentStatsUpdated> _segmentStatsUpdatedChannel;

    private ImmutableDictionary<byte[], IndexEntry> _index = ImmutableDictionary.Create<byte[], IndexEntry>(ByteArrayEqualityComparer.Shared);
    private readonly SegmentStatsTracker _statsTracker = new();

    public IndexActor(
        Channel<ChangesWritten> changesWrittenChannel,
        Channel<IndexUpdated> indexUpdatedChannel,
        Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel)
    {
        _changesWrittenChannel = changesWrittenChannel;
        _indexUpdatedChannel = indexUpdatedChannel;
        _segmentStatsUpdatedChannel = segmentStatsUpdatedChannel;
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
                    _statsTracker.OnIndexEntryDisplaced(payload.Key, previousIndexEntry);
                }

                indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
                _statsTracker.OnPayloadAdded(message.OutputSegment, payload);
            }
            
            // Process incoming tombstones. Tombstones will only ever increase the amount of "stale" bytes in the store.
            foreach (var tombstone in message.Tombstones)
            {
                if (indexBuilder.TryGetValue(tombstone, out var previousIndexEntry))
                {
                    _statsTracker.OnIndexEntryDisplaced(tombstone, previousIndexEntry);
                }

                indexBuilder.Remove(tombstone);
                _statsTracker.OnTombstoneAdded(message.OutputSegment, tombstone);
            }

            _index = indexBuilder.ToImmutable();

            await _indexUpdatedChannel.Writer.WriteAsync(
                new IndexUpdated(
                    Clock: message.Clock,
                    Index: _index));

            var stats = _statsTracker.ToImmutable();

            if (!stats.IsEmpty)
            {
                await _segmentStatsUpdatedChannel.Writer.WriteAsync(
                    new SegmentStatsUpdated(
                        Clock: message.Clock,
                        Stats: stats));
            }
        }

        // Propagate completion
        _indexUpdatedChannel.Writer.Complete();
        _segmentStatsUpdatedChannel.Writer.Complete();
    }
}
