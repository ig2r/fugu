using System.Collections.Immutable;

namespace Fugu.Utils;

public sealed class SegmentStatsTracker
{
    private readonly ImmutableSortedDictionary<Segment, SegmentStats>.Builder _statsBuilder;

    public SegmentStatsTracker()
    {
        var comparer = Comparer<Segment>.Create((x, y) =>
        {
            var cmp = Comparer<long>.Default.Compare(x.MinGeneration, y.MinGeneration);
            if (cmp != 0)
            {
                return cmp;
            }

            return Comparer<long>.Default.Compare(x.MaxGeneration, y.MaxGeneration);
        });

        _statsBuilder = ImmutableSortedDictionary.CreateBuilder<Segment, SegmentStats>(comparer);
    }

    public ImmutableSortedDictionary<Segment, SegmentStats> ToImmutable()
    {
        return _statsBuilder.ToImmutable();
    }

    public void OnIndexEntryDisplaced(byte[] key, IndexEntry indexEntry)
    {
        var byteCount = key.Length + indexEntry.Subrange.Length;
        var stats = _statsBuilder[indexEntry.Segment];

        _statsBuilder[indexEntry.Segment] = stats with
        {
            StaleBytes = stats.StaleBytes + byteCount,
        };
    }

    public void Add(SegmentStatsBuilder builder)
    {
        // Drop any current entries that are within the range of generations covered by builder.Segment.
        // This will happen only when merging a compacted segment.
        var replaced = _statsBuilder.Keys
            .SkipWhile(s => s.MinGeneration < builder.Segment.MinGeneration)
            .TakeWhile(s => s.MaxGeneration <= builder.Segment.MaxGeneration)
            .ToArray();

        if (replaced.Length > 0)
        {
            // Verify the before/after re. live bytes are equal
            var liveBytesBeforeCompaction = replaced.Sum(s => _statsBuilder[s].LiveBytes);
            var liveBytesAfterCompaction = builder.Stats.LiveBytes;

            if (liveBytesBeforeCompaction != liveBytesAfterCompaction)
            {
                throw new InvalidOperationException();
            }

            _statsBuilder.RemoveRange(replaced);
        }

        _statsBuilder[builder.Segment] = builder.Stats;
    }
}
