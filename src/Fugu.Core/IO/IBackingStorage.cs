namespace Fugu.IO;

public interface IBackingStorage
{
    ValueTask<IReadOnlyCollection<ISlab>> GetAllSlabsAsync();
    ValueTask<IWritableSlab> CreateSlabAsync();
    ValueTask RemoveSlabAsync(ISlab slab);
}
