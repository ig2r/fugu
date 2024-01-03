using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu.IO;

/// <summary>
/// Low-level utility to write a sequence of primitive values to a <see cref="Span{T}"/>.
/// </summary>
public ref struct SpanWriter
{
    private Span<byte> _span;

    public SpanWriter(Span<byte> span)
    {
        _span = span;
    }

    public void WriteByte(byte value)
    {
        MemoryMarshal.Write(_span, value);
        _span = _span.Slice(Unsafe.SizeOf<byte>());
    }

    public void WriteInt16(short value)
    {
        BinaryPrimitives.WriteInt16LittleEndian(_span, value);
        _span = _span.Slice(Unsafe.SizeOf<short>());
    }

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_span, value);
        _span = _span.Slice(Unsafe.SizeOf<int>());
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(_span, value);
        _span = _span.Slice(Unsafe.SizeOf<long>());
    }
}
