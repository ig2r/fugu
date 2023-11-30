using Fugu;
using Fugu.IO;
using System.Text;

const int Iterations = 100;
var storage = new InMemoryStorage();

await using (var store = await KeyValueStore.CreateAsync(storage))
{
    var random = new Random();

    for (var i = 0; i < Iterations; i++)
    {
        var key = Encoding.UTF8.GetBytes($"key:{random.Next(50)}");

        var changeSet = new ChangeSet
        {
            [key] = Encoding.UTF8.GetBytes($"value:{i}"),
        };

        await store.SaveAsync(changeSet);
    }
}
