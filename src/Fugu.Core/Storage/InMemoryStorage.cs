namespace Fugu.Storage;

public sealed class InMemoryStorage : IBackingStorage
{
    public ValueTask<IWritableStorageSlab> CreateSlabAsync()
    {
        var slab = new InMemorySlab();
        return ValueTask.FromResult<IWritableStorageSlab>(slab);
    }

    public ValueTask<IReadOnlyCollection<IStorageSlab>> GetAllSlabsAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask RemoveSlabAsync(IStorageSlab slab)
    {
        throw new NotImplementedException();
    }
}
