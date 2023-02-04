namespace Fugu.Core;

public sealed class KeyValueStore : IAsyncDisposable
{
    private KeyValueStore()
    {

    }

    public static ValueTask<KeyValueStore> CreateAsync(TableSet tableSet)
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask<Snapshot> GetSnapshotAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask WriteAsync(WriteBatch batch)
    {
        throw new NotImplementedException();
    }
}
