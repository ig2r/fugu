using Fugu.Utils;

namespace Fugu;

public sealed class ChangeSet
{
    private readonly Dictionary<byte[], ReadOnlyMemory<byte>> _payloads = new(ByteArrayEqualityComparer.Shared);
    private readonly HashSet<byte[]> _tombstones = new(ByteArrayEqualityComparer.Shared);

    // Name aligns with EF Core's DbSet. Not calling this Add() because:
    // - Dictionary<K, V>.Add() will throw if you try to add an existing key;
    // - HashSet<T>.Add() will not add an item if it already exists, and returns a bool to indicate success.
    public void AddOrUpdate(ReadOnlySpan<byte> key, ReadOnlyMemory<byte> value)
    {
        // Copy to ensure immutability
        var keyArray = key.ToArray();

        EnsureKeyNotYetTracked(keyArray);
        _payloads.Add(keyArray, value);
    }

    // Name aligns with EF Core's DbSet
    public void Remove(ReadOnlySpan<byte> key)
    {
        // Copy to ensure immutability
        var keyArray = key.ToArray();

        EnsureKeyNotYetTracked(keyArray);
        _tombstones.Add(keyArray);
    }

    // Convenience indexer to enable object initializer syntax
    public ReadOnlyMemory<byte> this[ReadOnlySpan<byte> key]
    {
        set => AddOrUpdate(key, value);
    }

    private void EnsureKeyNotYetTracked(byte[] key)
    {
        if (_payloads.ContainsKey(key))
        {
            throw new InvalidOperationException("ChangeSet already tracks the given key for add/update.");
        }

        if (_tombstones.Contains(key))
        {
            throw new InvalidOperationException("ChangeSet already tracks the given key for removal.");
        }
    }
}
