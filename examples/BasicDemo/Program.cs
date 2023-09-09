using Fugu;
using Fugu.Storage;

var storage = new InMemoryStorage();
var store = await KeyValueStore.CreateAsync(storage);

var changeSet = new ChangeSet
{
    ["foo"u8] = "Hello, world!"u8.ToArray(),
};

changeSet.Remove("bar"u8);


var slab = await storage.CreateSlabAsync();
await slab.Output.WriteAsync("Hello"u8.ToArray());

var length = slab.Length;
Console.WriteLine("Done");