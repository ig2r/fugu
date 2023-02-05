using System.Buffers;

namespace Fugu.Core.IO;

public abstract class Table
{
    public abstract ReadOnlySpan<byte> GetSpan(long start, int length);
    public abstract IBufferWriter<byte> GetWriter();
}
