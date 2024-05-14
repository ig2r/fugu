using Fugu;
using Fugu.IO;
using System.Text;

// Create a new key-value store instance that will keep its data in memory.
var storage = new InMemoryStorage();
await using var store = await KeyValueStore.CreateAsync(storage);

// Write some data to the store in an atomic transaction.
var changeSet = new ChangeSet
{
    ["greeting"u8] = Encoding.UTF8.GetBytes("Hello, world!"),
};

await store.SaveAsync(changeSet);

// Read back the data we just wrote and print it to the console.
using (var snapshot = await store.GetSnapshotAsync())
{
    var value = await snapshot.ReadAsync("greeting"u8);
    Console.WriteLine(Encoding.UTF8.GetString(value.Span));
}
