using System.Buffers;

namespace Fugu.Core.IO;

public class InMemoryTable : Table
{
    private readonly ArrayBufferWriter<byte> _arrayBufferWriter;

    public InMemoryTable(int capacity)
    {
        _arrayBufferWriter = new ArrayBufferWriter<byte>(capacity);
    }

    public override ReadOnlySpan<byte> GetSpan(long start, int length)
    {
        return _arrayBufferWriter.WrittenSpan.Slice((int)start, length);
    }

    public override IBufferWriter<byte> BufferWriter => _arrayBufferWriter;
}
