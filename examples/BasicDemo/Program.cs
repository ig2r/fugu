using Fugu.Core;

var tableSet = new TableSet();

await using (var store = await KeyValueStore.CreateAsync(tableSet))
{
    var batch = new WriteBatch();
    await store.WriteAsync(batch);
}