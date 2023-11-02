namespace Fugu;

internal interface ISnapshotOwner
{
    void OnSnapshotDisposed(Snapshot snapshot);
}
