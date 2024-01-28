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
    private ImmutableDictionary<byte[], IndexEntry> _index = ImmutableDictionary.Create<byte[], IndexEntry>(ByteArrayEqualityComparer.Shared);
    private readonly SegmentStatsTracker _statsTracker = new();

    public IndexActor(
        Channel<ChangesWritten> changesWrittenChannel,
        Channel<CompactionWritten> compactionWrittenChannel,
        Channel<IndexUpdated> indexUpdatedChannel,
        Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel)
    {
        _changesWrittenChannelReader = changesWrittenChannel.Reader;
        _compactionWrittenChannelReader = compactionWrittenChannel.Reader;
        _indexUpdatedChannelWriter = indexUpdatedChannel.Writer;
        _segmentStatsUpdatedChannelWriter = segmentStatsUpdatedChannel.Writer;
    }

    public async Task RunAsync()
    {
        await Task.WhenAll(
            ProcessChangesWrittenMessagesAsync(),
            ProcessCompactionWrittenMessagesAsync());
    }

    private async Task ProcessChangesWrittenMessagesAsync()
    {
        SegmentStatsBuilder? currentOutputSegmentStatsBuilder = null;

        while (await _changesWrittenChannelReader.WaitToReadAsync())
        {
            var message = await _changesWrittenChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                _clock = VectorClock.Max(_clock, message.Clock);

                // Did the output segment change? Ensure we have a matching SegmentStatsBuilder set up.
                if (message.OutputSegment != currentOutputSegmentStatsBuilder?.Segment)
                {
                    if (currentOutputSegmentStatsBuilder is not null)
                    {
                        _statsTracker.Add(currentOutputSegmentStatsBuilder);
                    }

                    currentOutputSegmentStatsBuilder = new SegmentStatsBuilder(message.OutputSegment);
                }

                var indexBuilder = _index.ToBuilder();

                // TODO: Figure out how to treat messages that happen due to compactions:
                // - Ensure updates don't clobber the index by replacing newer payloads.
                // - Ensure updates reflect properly in segment stats, i.e., stats for compacted source range
                //   get removed and replaced by stats for compacted output segment instead.

                // Process incoming payloads
                foreach (var payload in message.Payloads)
                {
                    // If this payload replaces an existing payload with the same key, mark the previous payload as stale
                    if (indexBuilder.TryGetValue(payload.Key, out var previousIndexEntry))
                    {
                        if (previousIndexEntry.Segment == currentOutputSegmentStatsBuilder.Segment)
                        {
                            // Mark entry as stale in current stats builder
                            currentOutputSegmentStatsBuilder.OnPayloadDisplaced(KeyValuePair.Create(payload.Key, previousIndexEntry.Subrange));
                        }
                        else
                        {
                            // Mark entry as stale in stats tracker
                            _statsTracker.OnIndexEntryDisplaced(payload.Key, previousIndexEntry);
                        }
                    }

                    indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
                    currentOutputSegmentStatsBuilder.OnPayloadAdded(payload);
                }

                // Process incoming tombstones. Tombstones will only ever increase the amount of "stale" bytes in the store.
                foreach (var tombstone in message.Tombstones)
                {
                    if (indexBuilder.TryGetValue(tombstone, out var previousIndexEntry))
                    {
                        if (previousIndexEntry.Segment == currentOutputSegmentStatsBuilder.Segment)
                        {
                            currentOutputSegmentStatsBuilder.OnPayloadDisplaced(KeyValuePair.Create(tombstone, previousIndexEntry.Subrange));
                        }
                        else
                        {
                            _statsTracker.OnIndexEntryDisplaced(tombstone, previousIndexEntry);
                        }
                    }

                    indexBuilder.Remove(tombstone);
                    currentOutputSegmentStatsBuilder.OnTombstoneAdded(tombstone);
                }

                _index = indexBuilder.ToImmutable();
                var stats = _statsTracker.ToImmutable();
                EnsureIndexAndStatsConsistent(_index, stats);

                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdated(
                        Clock: _clock,
                        Index: _index));

                if (!stats.IsEmpty)
                {
                    await _segmentStatsUpdatedChannelWriter.WriteAsync(
                        new SegmentStatsUpdated(
                            Clock: _clock,
                            Stats: stats,
                            Index: _index));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Propagate completion
        _indexUpdatedChannelWriter.Complete();
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
                var indexBuilder = _index.ToBuilder();

                // For every payload:
                // - If the incoming payload replaces another payload within the source generation range(!)
                //   in the index, then: update the index, track payload bytes as "live" in compacted segment.
                //   In theory, we could additionally mark the previous payload as "displaced". But since that
                //   source segment is going away from the tracker very soon anyways, why bother.
                // - Else, track payload bytes as "stale" in compacted segment.

                foreach (var payload in message.Changes.Payloads)
                {
                    statsBuilder.OnPayloadAdded(payload);

                    if (indexBuilder.TryGetValue(payload.Key, out var indexEntry) &&
                        indexEntry.Segment.MaxGeneration <= message.OutputSegment.MaxGeneration)
                    {
                        indexBuilder[payload.Key] = new IndexEntry(message.OutputSegment, payload.Value);
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

                _index = indexBuilder.ToImmutable();
                var stats = _statsTracker.ToImmutable();
                EnsureIndexAndStatsConsistent(_index, stats);

                await _indexUpdatedChannelWriter.WriteAsync(
                    new IndexUpdated(
                        Clock: _clock,
                        Index: _index));


                if (!stats.IsEmpty)
                {
                    await _segmentStatsUpdatedChannelWriter.WriteAsync(
                        new SegmentStatsUpdated(
                            Clock: _clock,
                            Stats: stats,
                            Index: _index));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private static void EnsureIndexAndStatsConsistent(
        ImmutableDictionary<byte[], IndexEntry> index,
        ImmutableSortedDictionary<Segment, SegmentStats> stats)
    {
        // Check 1: every generation in stats covered exactly once
        if (stats.Count > 0)
        {
            var maxGeneration = stats.Keys.Max(s => s.MaxGeneration);
            var generationOccurrences = new int[maxGeneration];

            foreach (var segment in stats.Keys)
            {
                for (var gen = segment.MinGeneration; gen <= segment.MaxGeneration; gen++)
                {
                    generationOccurrences[gen - 1]++;
                }
            }

            if (generationOccurrences.Any(g => g != 1))
            {
                throw new InvalidOperationException("Generation occurs an incorrect number of times in coverage counts.");
            }
        }

        // Check 2: index can contain at most one segment not present in stats (i.e., the current
        // output segment)
        var segmentsFromIndex = new HashSet<Segment>();
        foreach (var indexEntry in index.Values)
        {
            segmentsFromIndex.Add(indexEntry.Segment);
        }

        var segmentsFromStats = stats.Keys.ToHashSet();
        segmentsFromIndex.ExceptWith(segmentsFromStats);

        if (segmentsFromIndex.Count > 1)
        {
            throw new InvalidOperationException("There is more than one segment in index that is unaccounted for in stats.");
        }
    }
}
