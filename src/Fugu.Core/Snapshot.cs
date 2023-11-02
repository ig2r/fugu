namespace Fugu;

public sealed class Snapshot : IDisposable
{
    private readonly ISnapshotOwner _owner;

    internal Snapshot(ISnapshotOwner owner)
    {
        _owner = owner;
    }

    public void Dispose()
    {
        _owner.OnSnapshotDisposed(this);
    }
}
