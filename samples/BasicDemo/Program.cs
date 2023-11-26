using Fugu;
using Fugu.IO;
using System.Text;

const int Iterations = 100;
var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    for (var i = 0; i < Iterations; i++)
    {
        var key = Encoding.UTF8.GetBytes($"key:{i}");

        var changeSet = new ChangeSet
        {
            [key] = new byte[10],
        };

        await store.SaveAsync(changeSet);
    }
}

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    using var snapshot = await store.GetSnapshotAsync();

    for (var i = 0; i < Iterations; i++)
    {
        var key = Encoding.UTF8.GetBytes($"key:{i}");
        var data = await snapshot.ReadAsync(key);
    }
}

 Console.WriteLine("Done.");
