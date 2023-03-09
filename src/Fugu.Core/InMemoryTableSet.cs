using Fugu.Core.IO;

namespace Fugu.Core;

public class InMemoryTableSet : TableSet
{
    public override ValueTask<WritableTable> CreateTableAsync(long capacity)
    {
        var table = new InMemoryTable((int)capacity);
        return ValueTask.FromResult<WritableTable>(table);
    }
}
