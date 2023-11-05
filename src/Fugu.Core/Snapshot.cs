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
}
