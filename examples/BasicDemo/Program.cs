using Fugu.Core;
using System.Text;

var tableSet = new TableSet();

await using (var store = await KeyValueStore.CreateAsync(tableSet))
{
    var batch = new WriteBatch();

    batch[Encoding.UTF8.GetBytes("foo")] = Encoding.UTF8.GetBytes("Hello, world");
    batch[Encoding.UTF8.GetBytes("bar")] = Encoding.UTF8.GetBytes("This is another value");
    batch.Remove(Encoding.UTF8.GetBytes("baz"));

    await store.WriteAsync(batch);
}