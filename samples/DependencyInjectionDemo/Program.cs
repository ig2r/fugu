using Fugu;
using Fugu.DependencyInjection;
using Fugu.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder();

var storage = new InMemoryStorage();
builder.Services.AddKeyValueStore(storage);

var app = builder.Build();

var provider = app.Services.GetRequiredService<IKeyValueStoreProvider>();
var store = await provider.GetStoreAsync();

using (var snapshot = await store.GetSnapshotAsync())
{
}
