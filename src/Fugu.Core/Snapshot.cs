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

    public ValueTask ReadAsync(Memory<byte> buffer, Key key)
    {
        var indexEntry = _index[key];
        var payloadLocator = indexEntry.PayloadLocator;
        return indexEntry.Segment.Table.ReadAsync(buffer.Slice(0, payloadLocator.Size), payloadLocator.Start);
    }

    public void Dispose()
    {
        _onDispose(this);
    }
}
