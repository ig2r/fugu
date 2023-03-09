using System.IO.Pipelines;

namespace Fugu.Core.IO;

public abstract class WritableTable : Table
{
    public abstract PipeWriter Writer { get; }
}
