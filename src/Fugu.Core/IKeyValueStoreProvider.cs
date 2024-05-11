namespace Fugu;

public interface IKeyValueStoreProvider
{
    ValueTask<KeyValueStore> GetStoreAsync();
}
