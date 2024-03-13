using System.Collections.Immutable;

namespace Fugu.Utils;

/// <summary>
/// Keeps a tally of usage stats for all completed segments in the store.
/// </summary>
public sealed class StoreStatsTracker
{
    // Changes to this field will be relatively infrequent.
    private ImmutableArray<Segment> _keysArray = [];

    // Depending on load pattern, the stats for completed segments can update quite frequently.
    private readonly ImmutableList<SegmentStats>.Builder _statsBuilder = ImmutableList.CreateBuilder<SegmentStats>();

    public StoreStats ToImmutable()
    {
        return new StoreStats(_keysArray, _statsBuilder.ToImmutable());
    }

    private static Comparer<Segment> SegmentByMinGenerationComparer { get; } = Comparer<Segment>.Create((x, y) =>
    {
        return Comparer<long>.Default.Compare(x.MinGeneration, y.MinGeneration);
    });

    public void OnIndexEntryDisplaced(byte[] key, IndexEntry indexEntry)
    {
        var byteCount = key.Length + indexEntry.Subrange.Length;

        var index = _keysArray.BinarySearch(indexEntry.Segment, SegmentByMinGenerationComparer);
        var stats = _statsBuilder[index];
        _statsBuilder[index] = stats with
        {
            StaleBytes = stats.StaleBytes + byteCount,
        };
    }

    public void Add(SegmentStatsBuilder builder)
    {
        _keysArray = _keysArray.Add(builder.Segment);
        _statsBuilder.Add(builder.Stats);
    }

    public void MergeCompactedStats(SegmentStatsBuilder builder)
    {
        // Drop any current entries that are within the range of generations covered by builder.Segment.
        var index = _keysArray.BinarySearch(builder.Segment, SegmentByMinGenerationComparer);

        var length = 0;
        while (index + length < _keysArray.Length && _keysArray[index + length].MaxGeneration <= builder.Segment.MaxGeneration)
        {
            length++;
        }

        if (length == 0)
        {
            throw new InvalidOperationException("Failed to identify any segments to be replaced during compaction.");
        }

        // Verify the before/after re. live bytes are equal
        var liveBytesBeforeCompaction = _statsBuilder.Skip(index).Take(length).Sum(s => s.LiveBytes);
        var liveBytesAfterCompaction = builder.Stats.LiveBytes;

        if (liveBytesBeforeCompaction != liveBytesAfterCompaction)
        {
            throw new InvalidOperationException();
        }

        _statsBuilder.RemoveRange(index, length);
        _statsBuilder.Insert(index, builder.Stats);

        var keysArrayBuilder = _keysArray.ToBuilder();
        keysArrayBuilder.RemoveRange(index, length);
        keysArrayBuilder.Insert(index, builder.Segment);
        _keysArray = keysArrayBuilder.ToImmutable();
    }
}
