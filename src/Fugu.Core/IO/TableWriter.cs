using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu.Core.IO;

public class TableWriter
{
    private readonly IBufferWriter<byte> _bufferWriter;

    public TableWriter(IBufferWriter<byte> bufferWriter)
    {
        _bufferWriter = bufferWriter;
    }

    public long Position { get; private set; } = 0;

    public void Write<T>(in T value) where T : struct
    {
        var size = Unsafe.SizeOf<T>();
        var span = _bufferWriter.GetSpan(size);

        MemoryMarshal.Write(span, ref Unsafe.AsRef(in value));
        _bufferWriter.Advance(size);
        Position += size;
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        _bufferWriter.Write(value);
        Position += value.Length;
    }
}
