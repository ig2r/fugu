using Fugu.IO;
using Microsoft.Extensions.DependencyInjection;

namespace Fugu.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKeyValueStore(this IServiceCollection services, IBackingStorage storage)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(storage);

        services
            .AddOptions<KeyValueStoreOptions>()
            .Configure(options => options.Storage = storage);

        services.AddSingleton<IKeyValueStoreProvider, KeyValueStoreProvider>();

        return services;
    }
}
