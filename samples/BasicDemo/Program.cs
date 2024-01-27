using Fugu;
using Fugu.IO;
using System.Text;

const int Iterations = 10000;
var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    var random = new Random();

    for (var i = 0; i < Iterations; i++)
    {
        var payloadNo = random.Next(50);
        var tombstoneNo = random.Next(50);

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
        //await Task.Delay(TimeSpan.FromMilliseconds(1));
    }

    Console.WriteLine("Writing completed");
    await Task.Delay(TimeSpan.FromSeconds(5));

    using (var snapshot = await store.GetSnapshotAsync())
    {
        foreach (var key in snapshot.Keys)
        {
            Console.WriteLine($"Key: {Encoding.UTF8.GetString(key.ToArray())}");
        }
    }
}
