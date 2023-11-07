using Fugu;
using Fugu.IO;
using System.Text;

var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    var changeSet = new ChangeSet
    {
        ["foo"u8] = Encoding.UTF8.GetBytes("Hello, world!"),
        ["bar"u8] = new byte[0],
    };

    changeSet.Remove("baz"u8);
    await store.SaveAsync(changeSet);

    using var snapshot = await store.GetSnapshotAsync();
    var result = await snapshot.ReadAsync("foo"u8.ToArray());
}

Console.WriteLine("Done.");
