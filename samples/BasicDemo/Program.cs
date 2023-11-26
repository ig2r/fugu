using Fugu;
using Fugu.IO;

var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    var changeSet = new ChangeSet
    {
        ["foo"u8] = new byte[10],
        ["bar"u8] = Array.Empty<byte>(),
    };

    changeSet.Remove("baz"u8);
    await store.SaveAsync(changeSet);
}

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    using var snapshot = await store.GetSnapshotAsync();
    var result = await snapshot.ReadAsync("foo"u8);
}

Console.WriteLine("Done.");
