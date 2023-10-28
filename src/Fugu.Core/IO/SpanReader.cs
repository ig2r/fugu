using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu.IO;

/// <summary>
/// Low-level utility to read a sequence of primitive values from a <see cref="ReadOnlySpan{T}"/>.
/// </summary>
public ref struct SpanReader
{
    private ReadOnlySpan<byte> _span;

    public SpanReader(ReadOnlySpan<byte> span)
    {
        _span = span;
    }

    public byte ReadByte()
    {
        var value = MemoryMarshal.Read<byte>(_span);
        _span = _span.Slice(Unsafe.SizeOf<byte>());
        return value;
    }

    public short ReadInt16()
    {
        var value = BinaryPrimitives.ReadInt16LittleEndian(_span);
        _span = _span.Slice(Unsafe.SizeOf<short>());
        return value;
    }

    public int ReadInt32()
    {
        var value = BinaryPrimitives.ReadInt32LittleEndian(_span);
        _span = _span.Slice(Unsafe.SizeOf<int>());
        return value;
    }

    public long ReadInt64()
    {
        var value = BinaryPrimitives.ReadInt64LittleEndian(_span);
        _span = _span.Slice(Unsafe.SizeOf<long>());
        return value;
    }
}
