using Fugu.Channels;
using Fugu.Utils;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed partial class IndexActor
{
    private readonly SemaphoreSlim _semaphore = new(1);

    private readonly ChannelReader<ChangesWritten> _changesWrittenChannelReader;
    private readonly ChannelReader<CompactionWritten> _compactionWrittenChannelReader;
    private readonly ChannelWriter<IndexUpdated> _indexUpdatedChannelWriter;
    private readonly ChannelWriter<SegmentStatsUpdated> _segmentStatsUpdatedChannelWriter;

    private VectorClock _clock = default;
    private readonly ImmutableDictionary<byte[], IndexEntry>.Builder _indexBuilder =
        ImmutableDictionary.CreateBuilder<byte[], IndexEntry>(ByteArrayEqualityComparer.Shared);
    private readonly SegmentStatsTracker _statsTracker = new();

    public IndexActor(
        ChannelReader<ChangesWritten> changesWrittenChannelReader,
        ChannelReader<CompactionWritten> compactionWrittenChannelReader,
        ChannelWriter<IndexUpdated> indexUpdatedChannelWriter,
        ChannelWriter<SegmentStatsUpdated> segmentStatsUpdatedChannelWriter)
    {
        _changesWrittenChannelReader = changesWrittenChannelReader;
        _compactionWrittenChannelReader = compactionWrittenChannelReader;
        _indexUpdatedChannelWriter = indexUpdatedChannelWriter;
        _segmentStatsUpdatedChannelWriter = segmentStatsUpdatedChannelWriter;
    }

    public async Task RunAsync()
    {
        await Task.WhenAll(
            ProcessChangesWrittenMessagesAsync(),
            ProcessCompactionWrittenMessagesAsync());

        // Propagate completion
        _indexUpdatedChannelWriter.Complete();
    }

    private async Task ProcessChangesWrittenMessagesAsync()
    {
        SegmentStatsBuilder? outputSegmentStatsBuilder = null;

        while (await _changesWrittenChannelReader.WaitToReadAsync())
        {
            var message = await _changesWrittenChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                _clock = VectorClock.Max(_clock, message.Clock);

                // Did the output segment change? Ensure we have a matching SegmentStatsBuilder set up.
                if (message.OutputSegment != outputSegmentStatsBuilder?.Segment)
                {
                    if (outputSegmentStatsBuilder is not null)
                    {
                        _statsTracker.Add(outputSegmentStatsBuilder);
                    }

                    outputSegmentStatsBuilder = new SegmentStatsBuilder(message.OutputSegment);
                }

                // TODO: Figure out how to treat messages that happen due to compactions:
                // - Ensure updates don't clobber the index by replacing newer payloads.
                // - Ensure updates reflect properly in segment stats, i.e., stats for compacted source range
                //   get removed and replaced by stats for compacted output segment instead.

                // Process incoming payloads
                foreach (var payload in message.Payloads)
                {
                    // If this payload replaces an existing payload with the same key, mark the previous payload as stale
                    if (_indexBuilder.TryGetValue(payload.Key, out var previousIndexEntry))
                    {
                        if (previousIndexEntry.Segment == outputSegmentStatsBuilder.Segment)
                        {
                            // Mark entry as stale in current stats builder
                            outputSegmentStatsBuilder.OnPayloadDisplaced(KeyValuePair.Create(payload.Key, previousIndexEntry.Subrange));
                        }
                        else
                        {
                            // Mark entry as stale in stats tracker
                            _statsTracker.OnIndexEntryDisplaced(payload.Key, previousIndexEntry);
                        }
                    }

                    _indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
                    outputSegmentStatsBuilder.OnPayloadAdded(payload);
                }

                // Process incoming tombstones. Tombstones will only ever increase the amount of "stale" bytes in the store.
                foreach (var tombstone in message.Tombstones)
                {
                    if (_indexBuilder.TryGetValue(tombstone, out var previousIndexEntry))
                    {
                        if (previousIndexEntry.Segment == outputSegmentStatsBuilder.Segment)
                        {
                            outputSegmentStatsBuilder.OnPayloadDisplaced(KeyValuePair.Create(tombstone, previousIndexEntry.Subrange));
                        }
                        else
                        {
                            _statsTracker.OnIndexEntryDisplaced(tombstone, previousIndexEntry);
                        }
                    }

                    _indexBuilder.Remove(tombstone);
                    outputSegmentStatsBuilder.OnTombstoneAdded(tombstone);
                }

                var index = _indexBuilder.ToImmutable();
                var stats = _statsTracker.ToImmutable();

                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdated(Clock: _clock, Index: index));

                if (!stats.IsEmpty)
                {
                    await _segmentStatsUpdatedChannelWriter.WriteAsync(
                        new SegmentStatsUpdated(Clock: _clock, Stats: stats, Index: index));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Propagate completion towards compaction actor. By the time this happens, the actor loop processing
        // CompactionWritten messages will still be running.
        _segmentStatsUpdatedChannelWriter.Complete();
    }

    private async Task ProcessCompactionWrittenMessagesAsync()
    {
        while (await _compactionWrittenChannelReader.WaitToReadAsync())
        {
            var message = await _compactionWrittenChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                _clock = VectorClock.Max(_clock, message.Clock);

                var statsBuilder = new SegmentStatsBuilder(message.OutputSegment);

                // For every payload:
                // - If the incoming payload replaces another payload within the source generation range(!)
                //   in the index, then: update the index, track payload bytes as "live" in compacted segment.
                //   In theory, we could additionally mark the previous payload as "displaced". But since that
                //   source segment is going away from the tracker very soon anyways, why bother.
                // - Else, track payload bytes as "stale" in compacted segment.

                foreach (var payload in message.Changes.Payloads)
                {
                    statsBuilder.OnPayloadAdded(payload);

                    if (_indexBuilder.TryGetValue(payload.Key, out var indexEntry) &&
                        indexEntry.Segment.MaxGeneration <= message.OutputSegment.MaxGeneration)
                    {
                        _indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
                    }
                    else
                    {
                        statsBuilder.OnPayloadDisplaced(payload);
                    }
                }

                // For every tombstone:
                // - NEVER modify the index. Always track the tombstone bytes as "stale" right away.

                foreach (var tombstone in message.Changes.Tombstones)
                {
                    statsBuilder.OnTombstoneAdded(tombstone);
                }

                _statsTracker.Add(statsBuilder);

                var index = _indexBuilder.ToImmutable();
                var stats = _statsTracker.ToImmutable();

                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdated(Clock: _clock, Index: index));

                if (!stats.IsEmpty)
                {
                    // The following write may not succeed when the store is shutting down because the
                    // ChangesWritten processing loop will complete this channel after when it exits.
                    // We can safely ignore this scenario as we're shutting down anyways.
                    // In the regular case, we rely on the fact that the SegmentStatsUpdated channel is
                    // configured as a bounded channel with "latest-wins" replacement strategy, so even if
                    // there is an unread element still in the channel, this write is guaranteed to replace it.
                    _segmentStatsUpdatedChannelWriter.TryWrite(
                        new SegmentStatsUpdated(Clock: _clock, Stats: stats, Index: _indexBuilder));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}
