using Fugu;
using Fugu.IO;
using System.Text;

const int Iterations = 10000;
const int KeyspaceSize = 200;

var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    var random = new Random();

    for (var i = 0; i < Iterations; i++)
    {
        var payloadNo = random.Next(KeyspaceSize);
        var tombstoneNo = random.Next(KeyspaceSize);

        var key = Encoding.UTF8.GetBytes($"key:{payloadNo}");
        var changeSet = new ChangeSet
        {
            [key] = Encoding.UTF8.GetBytes($"value:{i}"),
        };

        if (payloadNo != tombstoneNo)
        {
            var tombstone = Encoding.UTF8.GetBytes($"key:{tombstoneNo}");
            changeSet.Remove(tombstone);
        }

        await store.SaveAsync(changeSet);
    }

    Console.WriteLine("Writing completed, pausing for 1 second");
    await Task.Delay(TimeSpan.FromSeconds(1));

    using (var snapshot = await store.GetSnapshotAsync())
    {
        foreach (var key in snapshot.Keys)
        {
            var value = await snapshot.ReadAsync(key.ToArray());
            Console.WriteLine($"Key: {Encoding.UTF8.GetString(key.ToArray())} - {Encoding.UTF8.GetString(value.Span)}");
        }
    }
}
