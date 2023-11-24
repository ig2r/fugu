namespace Fugu.IO;

public sealed class InMemoryStorage : IBackingStorage
{
    private readonly object _syncRoot = new();
    private readonly List<ISlab> _slabs = new();

    public ValueTask<IWritableSlab> CreateSlabAsync()
    {
        var slab = new InMemorySlab();

        lock (_syncRoot)
        {
            _slabs.Add(slab);
        }

        return ValueTask.FromResult<IWritableSlab>(slab);
    }

    public ValueTask<IReadOnlyCollection<ISlab>> GetAllSlabsAsync()
    {
        ISlab[] slabs;

        lock (_syncRoot)
        {
            slabs = _slabs.ToArray();
        }

        return ValueTask.FromResult<IReadOnlyCollection<ISlab>>(slabs);
    }

    public ValueTask RemoveSlabAsync(ISlab slab)
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
