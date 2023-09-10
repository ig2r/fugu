namespace Fugu.Storage;

public sealed class InMemoryStorage : IBackingStorage
{
    private readonly object _syncRoot = new();
    private readonly List<IStorageSlab> _slabs = new();

    public ValueTask<IWritableStorageSlab> CreateSlabAsync()
    {
        var slab = new InMemorySlab();

        lock (_syncRoot)
        {
            _slabs.Add(slab);
        }

        return ValueTask.FromResult<IWritableStorageSlab>(slab);
    }

    public ValueTask<IReadOnlyCollection<IStorageSlab>> GetAllSlabsAsync()
    {
        IStorageSlab[] slabs;

        lock (_syncRoot)
        {
            slabs = _slabs.ToArray();
        }

        return ValueTask.FromResult<IReadOnlyCollection<IStorageSlab>>(slabs);
    }

    public ValueTask RemoveSlabAsync(IStorageSlab slab)
    {
        bool removalSucceeded;

        lock (_syncRoot)
        {
            removalSucceeded = _slabs.Remove(slab);
        }

        if (!removalSucceeded)
        {
            return ValueTask.FromException(new InvalidOperationException("Slab not part of backing storage."));
        }

        return ValueTask.CompletedTask;
    }
}
