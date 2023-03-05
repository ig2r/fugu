namespace Fugu.Core.IO;

public abstract class WritableTable : Table
{
    public abstract Stream OutputStream { get; }
}
