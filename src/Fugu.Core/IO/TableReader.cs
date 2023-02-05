using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu.Core.IO;

public class TableReader
{
    private readonly Table _table;

    public TableReader(Table table)
	{
        _table = table;
    }

    public long Position { get; private set; } = 0;

    public ref readonly T Read<T>() where T : struct
    {
        var size = Unsafe.SizeOf<T>();
        var span = _table.GetSpan(Position, size);
        ref readonly var value = ref MemoryMarshal.AsRef<T>(span);

        Position += size;
        return ref value;
    }

    public ReadOnlySpan<byte> ReadBytes(int length)
    {
        var value = _table.GetSpan(Position, length);

        Position += length;
        return value;
    }
}
