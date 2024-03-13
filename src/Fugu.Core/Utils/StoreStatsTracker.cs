using System.Collections.Immutable;

namespace Fugu.Utils;

public sealed class StoreStatsTracker
{
    private readonly ImmutableArray<Segment>.Builder _keysBuilder = ImmutableArray.CreateBuilder<Segment>();
    private readonly ImmutableList<SegmentStats>.Builder _statsBuilder = ImmutableList.CreateBuilder<SegmentStats>();

    public StoreStats ToImmutable()
    {
        return new StoreStats(_keysBuilder.ToImmutable(), _statsBuilder.ToImmutable());
    }

    public void OnIndexEntryDisplaced(byte[] key, IndexEntry indexEntry)
    {
        var byteCount = key.Length + indexEntry.Subrange.Length;

        // TODO: Use BinarySearch instead
        var index = _keysBuilder.IndexOf(indexEntry.Segment);
        var stats = _statsBuilder[index];
        _statsBuilder[index] = stats with
        {
            StaleBytes = stats.StaleBytes + byteCount,
        };
    }

    public void Add(SegmentStatsBuilder builder)
    {
        // Drop any current entries that are within the range of generations covered by builder.Segment.
        // This will happen only when merging a compacted segment.

        // TODO: Use BinarySearch instead to find start and length.
        var start = 0;
        while (start < _keysBuilder.Count && _keysBuilder[start].MinGeneration < builder.Segment.MinGeneration)
        {
            start++;
        }

        var length = 0;
        while (start + length < _keysBuilder.Count && _keysBuilder[start + length].MaxGeneration <= builder.Segment.MaxGeneration)
        {
            length++;
        }

        if (length > 0)
        {
            // Verify the before/after re. live bytes are equal
            var liveBytesBeforeCompaction = _statsBuilder.Skip(start).Take(length).Sum(s => s.LiveBytes);
            var liveBytesAfterCompaction = builder.Stats.LiveBytes;

            if (liveBytesBeforeCompaction != liveBytesAfterCompaction)
            {
                throw new InvalidOperationException();
            }

            _keysBuilder.RemoveRange(start, length);
            _statsBuilder.RemoveRange(start, length);

            _keysBuilder.Insert(start, builder.Segment);
            _statsBuilder.Insert(start, builder.Stats);
        }
        else
        {
            _keysBuilder.Add(builder.Segment);
            _statsBuilder.Add(builder.Stats);
        }
    }
}
