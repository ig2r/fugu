using Fugu.Utils;

namespace Fugu;

public sealed class Snapshot : IDisposable
{
    private readonly ISnapshotOwner _owner;
    private readonly IReadOnlyDictionary<byte[], IndexEntry> _index;

    internal Snapshot(ISnapshotOwner owner, IReadOnlyDictionary<byte[], IndexEntry> index)
    {
        _owner = owner;
        _index = index;
    }

    public void Dispose()
    {
        _owner.OnSnapshotDisposed(this);
    }

    public bool ContainsKey(byte[] key)
    {
        return _index.ContainsKey(key);
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReadAsync(byte[] key)
    {
        var indexEntry = _index[key];
        var buffer = new byte[indexEntry.Length];
        await indexEntry.Segment.Slab.ReadAsync(buffer, indexEntry.Offset);
        return buffer;
    }
}
