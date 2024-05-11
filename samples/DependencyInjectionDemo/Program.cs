using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Fugu;
using Fugu.IO;

var builder = Host.CreateApplicationBuilder();

builder.Services
    .AddOptions<KeyValueStoreOptions>()
    .Configure(options => options.Storage = new InMemoryStorage());

builder.Services.AddSingleton<IKeyValueStoreProvider, KeyValueStoreProvider>();

var app = builder.Build();

var provider = app.Services.GetRequiredService<IKeyValueStoreProvider>();
var store = await provider.GetStoreAsync();

using (var snapshot = await store.GetSnapshotAsync())
{
}
