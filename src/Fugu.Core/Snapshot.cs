namespace Fugu.Core;

public sealed class Snapshot : IDisposable
{
    private readonly Index _index;
    private readonly Action<Snapshot> _onDispose;

    public Snapshot(Index index, Action<Snapshot> onDispose)
    {
        _index = index;
        _onDispose = onDispose;
    }

    public void Dispose()
    {
        _onDispose(this);
    }
}
