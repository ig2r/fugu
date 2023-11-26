namespace Fugu.Core.Tests;

public class ChangeSetTests
{
    [Fact]
    public void AddOrUpdate_CalledTwiceWithSameKey_ThrowsInvalidOperationException()
    {
        var changeSet = new ChangeSet();
        changeSet.AddOrUpdate("foo"u8, Array.Empty<byte>());

        var ex = Record.Exception(() => changeSet.AddOrUpdate("foo"u8, Array.Empty<byte>()));

        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public void Remove_KeyAlreadyTrackedUsingAddedOrUpdated_ThrowsInvalidOperationException()
    {
        var changeSet = new ChangeSet();
        changeSet.AddOrUpdate("foo"u8, Array.Empty<byte>());

        var ex = Record.Exception(() => changeSet.Remove("foo"u8));

        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }
}
