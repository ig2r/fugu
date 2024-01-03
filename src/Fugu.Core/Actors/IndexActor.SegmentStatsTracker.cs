using Fugu.Utils;
using System.Collections.Immutable;

namespace Fugu.Actors;

public sealed partial class IndexActor
{
    private sealed class SegmentStatsTracker
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

        public void OnPayloadAdded(Segment segment, KeyValuePair<byte[], SlabSubrange> payload)
        {
            var byteCount = payload.Key.Length + payload.Value.Length;
            var stats = _statsBuilder.TryGetValue(segment, out var s) ? s : new();

            _statsBuilder[segment] = stats with
            {
                TotalBytes = stats.TotalBytes + byteCount,
            };
        }

        public void OnTombstoneAdded(Segment segment, byte[] tombstone)
        {
            var byteCount = tombstone.Length;
            var stats = _statsBuilder.TryGetValue(segment, out var s) ? s : new();

            _statsBuilder[segment] = stats with
            {
                TotalBytes = stats.TotalBytes + byteCount,
                StaleBytes = stats.StaleBytes + byteCount,
            };
        }
    }
}
