using System.Collections.Immutable;

namespace Fugu.Utils;

public sealed class SegmentStatsTracker
{
    private readonly ImmutableSortedDictionary<Segment, SegmentStats>.Builder _statsBuilder;
    private Segment? _outputSegment = null;
    private SegmentStats _outputSegmentStats = new();

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

        if (indexEntry.Segment == _outputSegment)
        {
            _outputSegmentStats = _outputSegmentStats with
            {
                StaleBytes = _outputSegmentStats.StaleBytes + byteCount,
            };
        }
        else
        {
            var stats = _statsBuilder[indexEntry.Segment];

            _statsBuilder[indexEntry.Segment] = stats with
            {
                StaleBytes = stats.StaleBytes + byteCount,
            };
        }
    }

    public void OnPayloadAdded(Segment segment, KeyValuePair<byte[], SlabSubrange> payload)
    {
        UseOutputSegment(segment);

        var byteCount = payload.Key.Length + payload.Value.Length;
        _outputSegmentStats = _outputSegmentStats with
        {
            TotalBytes = _outputSegmentStats.TotalBytes + byteCount,
        };
    }

    public void OnTombstoneAdded(Segment segment, byte[] tombstone)
    {
        UseOutputSegment(segment);

        var byteCount = tombstone.Length;
        _outputSegmentStats = _outputSegmentStats with
        {
            TotalBytes = _outputSegmentStats.TotalBytes + byteCount,
            StaleBytes = _outputSegmentStats.StaleBytes + byteCount,
        };
    }

    private void UseOutputSegment(Segment segment)
    {
        if (_outputSegment == segment)
        {
            return;
        }

        if (_outputSegment is not null)
        {
            _statsBuilder[_outputSegment] = _outputSegmentStats;
        }

        _outputSegment = segment;
        _outputSegmentStats = new();
    }
}
