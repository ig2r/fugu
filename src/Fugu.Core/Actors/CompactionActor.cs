using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly SemaphoreSlim _semaphore = new(1);

    private readonly IBackingStorage _storage;
    private readonly ChannelReader<SegmentStatsUpdated> _segmentStatsUpdatedChannelReader;
    private readonly ChannelReader<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannelReader;
    private readonly ChannelWriter<CompactionWritten> _compactionWrittenChannelWriter;
    private readonly ChannelWriter<SegmentsCompacted> _segmentsCompactedChannelWriter;

    private readonly Queue<PendingCompactionCleanup> _pendingCompactionCleanups = new();

    public CompactionActor(
        IBackingStorage storage,
        ChannelReader<SegmentStatsUpdated> segmentStatsUpdatedChannelReader,
        ChannelReader<OldestObservableSnapshotChanged> oldestObservableSnapshotChangedChannelReader,
        ChannelWriter<CompactionWritten> compactionWrittenChannelWriter,
        ChannelWriter<SegmentsCompacted> segmentsCompactedChannelWriter)
    {
        _storage = storage;
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

            // Discard messages while we're still waiting for the effects of a previous compaction to become observable
            if (message.Clock.Compaction < compactionClockThreshold)
            {
                continue;
            }

            await _semaphore.WaitAsync();

            try
            {
                // Geometric series characterized by two parameters a and r:
                const double a = 100;       // Coefficient, also the size of slab #0
                const double r = 1.5;       // Common ratio, indicates by how much each added slab should be bigger than the last

                // Given the current number n of non-output segments in the store, calculate idealized capacity of the store as the
                // cumulative sum of an n-element (a, r) geometric series:
                var n = message.Stats.Count;
                var capacity = a * (1 - Math.Pow(r, n)) / (1 - r);

                // Calculate actual sum of "live" bytes:
                var totalLiveBytes = message.Stats.Sum(s => s.Value.LiveBytes);

                // We define utilization as the ratio of "live" bytes to idealized capacity given the current number of non-output
                // segments. If utilization drops too far below 1.0, this indicates that the store is using too many segments for
                // the amount of payload data it holds, and should be compacted to flush out stale data.
                var utilization = totalLiveBytes / capacity;

                // Setting the utilization threshold at 0.5 means that up to 50% of usable space within segments can be taken up
                // by stale data before a compaction is triggered. Choosing a higher threshold will allow less wasted space, at the
                // cost of higher write amplification. Choosing a lower threshold will reduce the frequency of compactions, but could
                // result in more space being wasted by stale data.
                if (utilization < 0.5)
                {
                    // We need to compact. Identify a suitable range of source segments.
                    // For each candidate range of segments, we are interested in two numbers:
                    // - By how much compacting these segments will reduce the idealized capacity; this is dependent only on n.
                    // - How much data we will likely need to copy during the compaction. Live bytes in source segments for sure;
                    //   potentially some "stale" bytes as well if they represent tombstones for values that may still exist in
                    //   previous segments.
                    // The ratio of both numbers yields the "efficiency" of compacting a specific candidate range, i.e., by how much
                    // each copied byte will be able to improve the utilization figure.

                    // TODO: Be smart about this, for now we always choose the first two segments as compaction inputs.

                    var sourceStats = message.Stats.Take(2).ToArray();

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
                        // Special case: if the compacted range starts at the very beginning, no need to
                        // bring along tombstones.
                        // TODO: Once we scan previous segments' Bloom filters for this key, this special
                        // case will go away organically.
                        if (minGeneration > 1)
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
            compactedTombstones.ToArray());

        return (segmentWriter.Segment, changes);
    }
}
