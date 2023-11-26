using Fugu.IO;
using System.Text;

namespace Fugu.Core.Tests;

public class KeyValueStoreTests
{
    [Fact]
    public async Task KeyValueStore_SavedChanges_CanBeReadFromSnapshot()
    {
        var storage = new InMemoryStorage();
        await using var store = await KeyValueStore.CreateAsync(storage);

        var changeSet = new ChangeSet
        {
            ["foo"u8] = Encoding.UTF8.GetBytes("Hello, world"),
        };

        await store.SaveAsync(changeSet);

        using var snapshot = await store.GetSnapshotAsync();
        var retrievedValue = await snapshot.ReadAsync("foo"u8);

        Assert.Equal("Hello, world", Encoding.UTF8.GetString(retrievedValue.ToArray()));
    }
}
