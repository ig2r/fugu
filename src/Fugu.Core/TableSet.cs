using Fugu.Core.IO;

namespace Fugu.Core;

public abstract class TableSet
{
    public abstract ValueTask<WritableTable> CreateTableAsync(long capacity);
}
