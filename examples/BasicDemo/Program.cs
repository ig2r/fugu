using Fugu.Core;
using System.Text;

var tableSet = new InMemoryTableSet();

await using (var store = await KeyValueStore.CreateAsync(tableSet))
{
    // First transaction
    {
        var batch = new WriteBatch();

        batch[Encoding.UTF8.GetBytes("foo")] = Encoding.UTF8.GetBytes("Hello, world");
        batch[Encoding.UTF8.GetBytes("bar")] = Encoding.UTF8.GetBytes("This is another value");
        batch.Remove(Encoding.UTF8.GetBytes("baz"));

        await store.WriteAsync(batch);
    }

    // Second transaction
    {
        var batch = new WriteBatch();

        batch[Encoding.UTF8.GetBytes("foo")] = Encoding.UTF8.GetBytes("HELLO");
        batch.Remove(Encoding.UTF8.GetBytes("bar"));

        await store.WriteAsync(batch);
    }

    // Read results
    {
        var snapshot = await store.GetSnapshotAsync();
    }
}