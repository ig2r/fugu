using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly SemaphoreSlim _semaphore = new(1);

    private readonly IBackingStorage _storage;
    private readonly BalancingStrategy _balancingStrategy;
    private readonly ChannelReader<SegmentStatsUpdated> _segmentStatsUpdatedChannelReader;
    private readonly ChannelReader<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannelReader;
    private readonly ChannelWriter<CompactionWritten> _compactionWrittenChannelWriter;
    private readonly ChannelWriter<SegmentsCompacted> _segmentsCompactedChannelWriter;

    private readonly Queue<PendingCompactionCleanup> _pendingCompactionCleanups = new();

    public CompactionActor(
        IBackingStorage storage,
        BalancingStrategy balancingStrategy,
        ChannelReader<SegmentStatsUpdated> segmentStatsUpdatedChannelReader,
        ChannelReader<OldestObservableSnapshotChanged> oldestObservableSnapshotChangedChannelReader,
        ChannelWriter<CompactionWritten> compactionWrittenChannelWriter,
        ChannelWriter<SegmentsCompacted> segmentsCompactedChannelWriter)
    {
        _storage = storage;
        _balancingStrategy = balancingStrategy;
        _segmentStatsUpdatedChannelReader = segmentStatsUpdatedChannelReader;
        _oldestObservableSnapshotChangedChannelReader = oldestObservableSnapshotChangedChannelReader;
        _compactionWrittenChannelWriter = compactionWrittenChannelWriter;
        _segmentsCompactedChannelWriter = segmentsCompactedChannelWriter;
    }

    public async Task RunAsync()
    {
            await Task.WhenAll(
                ProcessSegmentStatsUpdatedMessagesAsync(),
                ProcessOldestObservableSnapshotChangedMessagesAsync());

        // Propagate completion
        _segmentsCompactedChannelWriter.Complete();
    }

    private async Task ProcessSegmentStatsUpdatedMessagesAsync()
    {
        long compactionClockThreshold = 0;

        while (await _segmentStatsUpdatedChannelReader.WaitToReadAsync())
        {
            var message = await _segmentStatsUpdatedChannelReader.ReadAsync();

            // Discard messages while we're still waiting for the effects of a previous compaction to become observable.
            if (message.Clock.Compaction < compactionClockThreshold)
            {
                continue;
            }

            // Cannot compact if there is only a single completed segment.
            if (message.Stats.Count < 2)
            {
                continue;
            }

            await _semaphore.WaitAsync();

            try
            {
                // Given the current number n of non-output segments in the store, calculate idealized capacity of the store.
                // Then derive the utilization ratio of idealized capacity vs. actual total "live" bytes across all non-output
                // segments. If this ratio drops too far below 1.0, the store is using too many segments for the amount of payload
                // data it holds and should be compacted to flush out stale data.
                float capacity = _balancingStrategy.GetIdealizedCapacity(message.Stats.Count);
                float totalLiveBytes = message.Stats.Sum(s => s.Value.LiveBytes);
                float utilization = totalLiveBytes / capacity;

                // Setting the utilization threshold at 0.5 means that up to 50% of usable space within segments can be taken up
                // by stale data before a compaction is triggered. Choosing a higher threshold will allow less wasted space, at the
                // cost of higher write amplification. Choosing a lower threshold will reduce the frequency of compactions, but could
                // result in more space being wasted by stale data.
                if (utilization < 0.5)
                {
                    // We need to compact. Identify a suitable range of source segments.
                    var statsArray = message.Stats.Values.ToArray();
                    var (start, length) = ChooseCompactionSourceRange(statsArray);
                    var sourceStats = message.Stats.Skip(start).Take(length).ToArray();

                    compactionClockThreshold++;

                    // Run actual compaction
                    var outputSlab = await _storage.CreateSlabAsync();
                    var (compactedSegment, compactedChanges) = await CompactSegmentsAsync(
                        outputSlab,
                        sourceStats.Select(kvp => kvp.Key).ToArray(),
                        message.Index);

                    // Tell index actor about compaction so that it'll reflect the changes in the main index.
                    await _compactionWrittenChannelWriter.WriteAsync(
                        new CompactionWritten(
                            Clock: message.Clock with { Compaction = compactionClockThreshold },
                            OutputSegment: compactedSegment,
                            Changes: compactedChanges));

                    // Determine by how much the store's total capacity changes with this compaction.
                    var oldCapacity = sourceStats.Sum(s => s.Value.TotalBytes);
                    var newCapacity = ChangeSetUtils.GetDataBytes(compactedChanges);
                    var capacityChange = newCapacity - oldCapacity;

                    // Cannot delete the source segments right away because there might be active snapshots
                    // that reference it. Instead, add them to a list and delete them when SnapshotsActor signals
                    // that no states before the current compaction clock are observable in snapshots anymore.
                    _pendingCompactionCleanups.Enqueue(new(
                        CompactionClock: compactionClockThreshold,
                        Segments: sourceStats.Select(kvp => kvp.Key).ToArray(),
                        CapacityChange: capacityChange
                    ));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Propagate completion, there will be no further compactions.
        _compactionWrittenChannelWriter.Complete();
    }

    private async Task ProcessOldestObservableSnapshotChangedMessagesAsync()
    {
        while (await _oldestObservableSnapshotChangedChannelReader.WaitToReadAsync())
        {
            var message = await _oldestObservableSnapshotChangedChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                while (_pendingCompactionCleanups.TryPeek(out var cleanup) && message.Clock.Compaction >= cleanup.CompactionClock)
                {
                    _pendingCompactionCleanups.Dequeue();

                    foreach (var segment in cleanup.Segments)
                    {
                        await _storage.RemoveSlabAsync(segment.Slab);
                    }

                    // Tell allocation actor that the store's total capacity has decreased, so that it will
                    // account for it by making future segments smaller again.
                    await _segmentsCompactedChannelWriter.WriteAsync(
                        new SegmentsCompacted(CapacityChange: cleanup.CapacityChange));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    /// <summary>
    /// Picks the most favorable sub-range of segments for compaction.
    /// </summary>
    /// <param name="stats">Segment usage stats, provided by IndexActor.</param>
    /// <returns>Start and length of a range of segments to compact.</returns>
    private static (int Start, int Length) ChooseCompactionSourceRange(SegmentStats[] stats)
    {
        // Look for the 2- or 3-element range of segments that has the most skewed ratio between "live" vs.
        // "stale" bytes, that is, in which the amount of stale bytes that can be stripped away per every
        // live byte copied is maximized.

        var maxRatio = 0f;
        int start = 0;
        int length = 2;

        for (var i = 0; i < stats.Length - 1; i++)
        {
            for (var k = 2; k <= 3 && i + k <= stats.Length; k++)
            {
                var (totalBytes, staleBytes) = stats.Skip(i).Take(k).Aggregate(
                    (TotalBytes: 0f, StaleBytes: 0f),
                    (acc, x) => (acc.TotalBytes + x.TotalBytes, acc.StaleBytes + x.StaleBytes));

                // Apply Laplacian +1 smoothing to avoid division by zero.
                var ratio = staleBytes / (totalBytes + 1);
                if (ratio > maxRatio)
                {
                    maxRatio = ratio;
                    start = i;
                    length = k;
                }
            }
        }

        return (start, length);
    }

    private static async ValueTask<(Segment Segment, ChangeSetCoordinates Changes)> CompactSegmentsAsync(
        IWritableSlab outputSlab,
        Segment[] sourceSegments,
        IReadOnlyDictionary<byte[], IndexEntry> index)
    {
        var minGeneration = sourceSegments.Min(s => s.MinGeneration);
        var maxGeneration = sourceSegments.Max(s => s.MaxGeneration);
        var segmentWriter = await SegmentWriter.CreateAsync(outputSlab, minGeneration, maxGeneration);

        // Tracks payloads and tombstones we've already written to the output segment.
        var compactedPayloads = new List<KeyValuePair<byte[], SlabSubrange>>();
        var compactedTombstones = new HashSet<byte[]>(ByteArrayEqualityComparer.Shared);

        foreach (var sourceSegment in sourceSegments)
        {
            var segmentReader = await SegmentReader.CreateAsync(sourceSegment.Slab);

            // For now, we'll create one change set per source segment. Using the ChangeSet
            // type means we hold the source payload values in memory, though -- in the future,
            // better to track just the payload coordinates, then read/write from source to
            // output using vectored I/O when done?
            var toWrite = new ChangeSet();

            await foreach (var changeSet in segmentReader.ReadChangeSetsAsync())
            {
                foreach (var payload in changeSet.Payloads)
                {
                    if (index.TryGetValue(payload.Key, out var indexEntry) &&
                        sourceSegment == indexEntry.Segment &&
                        payload.Value.Offset == indexEntry.Subrange.Offset)
                    {
                        // TODO: Use pooled buffer, don't allocate a new buffer each time
                        var buffer = new byte[payload.Value.Length];
                        await sourceSegment.Slab.ReadAsync(buffer, payload.Value.Offset);
                        toWrite.AddOrUpdate(payload.Key, buffer);
                    }
                }

                // Only bother with tombstones if the compacted range doesn't begin at the very first generation.
                // TODO: We will no longer need this check once we look at Bloom filters to detect potential
                // payloads for this key in previous segments.
                if (minGeneration > 1)
                {
                    foreach (var tombstone in changeSet.Tombstones)
                    {
                        // TODO: Checking index isn't enough; also check Bloom filters of preceding segments
                        // if any of those might contain a payload for that key. Skip if Bloom filters show
                        // that no preceding segment has that key.
                        // For now, we can only skip a tombstone if there's an active payload in the index
                        // that supersedes it.
                        if (!index.ContainsKey(tombstone) &&
                            !compactedTombstones.Contains(tombstone))
                        {
                            toWrite.Remove(tombstone);
                            compactedTombstones.Add(tombstone);
                        }
                    }
                }
            }

            if (toWrite.Payloads.Count > 0 || toWrite.Tombstones.Count > 0)
            {
                var coordinates = await segmentWriter.WriteChangeSetAsync(toWrite);
                compactedPayloads.AddRange(coordinates.Payloads);
            }
        }

        await segmentWriter.CompleteAsync();

        // Act like we wrote one big change set. Caller doesn't care.
        var changes = new ChangeSetCoordinates(
            compactedPayloads,
            [.. compactedTombstones]);

        return (segmentWriter.Segment, changes);
    }
}
