using Fugu.Storage;

namespace Fugu;

public class KeyValueStore
{
    private KeyValueStore(IBackingStorage storage)
    {
        
    }

    public static ValueTask<KeyValueStore> CreateAsync(IBackingStorage storage)
    {
        var store = new KeyValueStore(storage);
        return ValueTask.FromResult(store);
    }
}
