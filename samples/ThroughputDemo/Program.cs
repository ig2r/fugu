using Fugu;
using Fugu.IO;
using System.Diagnostics;
using System.Text;

const int Iterations = 100_000;
const int ConcurrentWriters = 8;

var storage = new InMemoryStorage();
await using var store = await KeyValueStore.CreateAsync(storage);

var tasks = new HashSet<Task>();
var random = new Random();

var stopwatch = Stopwatch.StartNew();

for (var i = 0; i < Iterations; i++)
{
    while (tasks.Count < ConcurrentWriters)
    {
        var key = Encoding.UTF8.GetBytes($"key:{random.Next(200)}");

        var changeSet = new ChangeSet
        {
            [key] = Encoding.UTF8.GetBytes("DATA"),
        };

        var t = store.SaveAsync(changeSet);
        tasks.Add(t.AsTask());
    }

    var completedTask = await Task.WhenAny(tasks);
    tasks.Remove(completedTask);
}

await Task.WhenAll(tasks);

stopwatch.Stop();
Console.WriteLine($"Finished in {stopwatch.ElapsedMilliseconds} ms");
