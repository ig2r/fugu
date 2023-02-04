using System.Buffers;

namespace Fugu.Core.IO;

public class Table
{
    public ReadOnlySpan<byte> GetSpan(long start, int length)
    {
        throw new NotImplementedException();
    }

    public IBufferWriter<byte> GetWriter()
    {
        throw new NotImplementedException();
    }
}
