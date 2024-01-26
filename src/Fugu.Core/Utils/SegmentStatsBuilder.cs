namespace Fugu.Utils;

public sealed class SegmentStatsBuilder
{
    // Track unique keys of payloads that appear in the associated segment. When the segment has been completed,
    // these keys may require that tombstones in following segments cannot be flushed out during compaction,
    // because they must continue to suppress this payload.
    private readonly HashSet<byte[]> _payloadKeys = new(ByteArrayEqualityComparer.Shared);
    private SegmentStats _stats = new();

    public SegmentStatsBuilder(Segment segment)
    {
        Segment = segment;
    }

    public Segment Segment { get; }
    public HashSet<byte[]> PayloadKeys => _payloadKeys;
    public SegmentStats Stats => _stats;

    public void OnPayloadAdded(KeyValuePair<byte[], SlabSubrange> payload)
    {
        var byteCount = payload.Key.Length + payload.Value.Length;
        _stats = _stats with
        {
            TotalBytes = _stats.TotalBytes + byteCount,
        };

        _payloadKeys.Add(payload.Key);
    }

    public void OnTombstoneAdded(byte[] tombstone)
    {
        var byteCount = tombstone.Length;
        _stats = _stats with
        {
            TotalBytes = _stats.TotalBytes + byteCount,
            StaleBytes = _stats.StaleBytes + byteCount,
        };
    }

    public void OnPayloadDisplaced(KeyValuePair<byte[], SlabSubrange> payload)
    {
        if (_payloadKeys.Remove(payload.Key))
        {
            var byteCount = payload.Key.Length + payload.Value.Length;
            _stats = _stats with
            {
                StaleBytes = _stats.StaleBytes + byteCount,
            };
        }
    }
}
