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

    public ReadOnlySpan<byte> this[Key key]
    {
        get
        {
            var indexEntry = _index[key];
            var payloadLocator = indexEntry.PayloadLocator;
            var span = indexEntry.Segment.Table.GetSpan(payloadLocator.Start, payloadLocator.Size);
            return span;
        }
    }

    public void Dispose()
    {
        _onDispose(this);
    }
}
