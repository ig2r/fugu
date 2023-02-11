using System.Buffers;

namespace Fugu.Core.IO;

public abstract class ReadOnlyTable
{
    public abstract ReadOnlySpan<byte> GetSpan(long start, int length);
}
