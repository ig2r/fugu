using Fugu.IO;
using System.Text;

namespace Fugu.Core.Tests;

public class KeyValueStoreTests
{
    [Fact]
    public async Task SaveAsync_ChangesAreVisibleInFutureSnapshots()
    {
        // Verifies that when a change set has been written, its values are immediately visible in
        // future snapshots.
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

    [Fact]
    public async Task SaveAsync_PreexistingSnapshot_StillSeesOldValues()
    {
        // Verifies that when a snapshot is open, it will continue to see the values at the
        // time of its creation even if the store is modified in the meantime.
        var storage = new InMemoryStorage();
        await using var store = await KeyValueStore.CreateAsync(storage);

        await store.SaveAsync(new()
        {
            ["foo"u8] = Encoding.UTF8.GetBytes("Initial value"),
        });

        using var snapshot = await store.GetSnapshotAsync();

        await store.SaveAsync(new()
        {
            ["foo"u8] = Encoding.UTF8.GetBytes("Updated value"),
        });

        var retrievedValue = await snapshot.ReadAsync("foo"u8);
        Assert.Equal("Initial value", Encoding.UTF8.GetString(retrievedValue.ToArray()));
    }
}
