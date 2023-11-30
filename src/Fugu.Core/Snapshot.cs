using Fugu.Utils;
using System.Collections;

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

    public IEnumerable<IReadOnlyList<byte>> Keys => _index.Keys;

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
        var buffer = new byte[indexEntry.Subrange.Length];
        await indexEntry.Segment.Slab.ReadAsync(buffer, indexEntry.Subrange.Offset);
        return buffer;
    }

    // Convenience overload to make key handling symmetric to ChangeSet methods, which also accept a
    // ReadOnlySpan-typed key parameter.
    public ValueTask<ReadOnlyMemory<byte>> ReadAsync(ReadOnlySpan<byte> key)
    {
        return ReadAsync(key.ToArray());
    }
}
