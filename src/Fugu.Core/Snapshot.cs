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

    public bool TryGetLength(Key key, out int length)
    {
        if (!_index.TryGetValue(key, out var indexEntry))
        {
            length = 0;
            return false;
        }

        length = indexEntry.PayloadLocator.Size;
        return true;
    }

    public ValueTask ReadAsync(Key key, Memory<byte> buffer)
    {
        var indexEntry = _index[key];
        var payloadLocator = indexEntry.PayloadLocator;
        return indexEntry.Segment.Table.ReadAsync(buffer[..payloadLocator.Size], payloadLocator.Start);
    }

    public void Dispose()
    {
        _onDispose(this);
    }
}
