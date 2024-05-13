namespace Fugu.DependencyInjection;

public interface IKeyValueStoreProvider
{
    ValueTask<KeyValueStore> GetStoreAsync();
}
