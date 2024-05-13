using Microsoft.Extensions.Options;

namespace Fugu.DependencyInjection;

internal sealed class KeyValueStoreProvider : IAsyncDisposable, IKeyValueStoreProvider
{
    private readonly Lazy<Task<KeyValueStore>> _lazyKeyValueStore;

    public KeyValueStoreProvider(IOptions<KeyValueStoreOptions> options)
    {
        _lazyKeyValueStore = new(() => KeyValueStore.CreateAsync(options.Value.Storage));
    }

    public ValueTask<KeyValueStore> GetStoreAsync()
    {
        var task = _lazyKeyValueStore.Value;
        return new ValueTask<KeyValueStore>(task);
    }

    public async ValueTask DisposeAsync()
    {
        var store = await _lazyKeyValueStore.Value;
        await store.DisposeAsync();
    }
}
