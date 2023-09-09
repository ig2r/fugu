namespace Fugu.Storage;

public interface IBackingStorage
{
    ValueTask<IReadOnlyCollection<IStorageSlab>> GetAllSlabsAsync();
    ValueTask<IWritableStorageSlab> CreateSlabAsync();
    ValueTask RemoveSlabAsync(IStorageSlab slab);
}
