using System.Buffers;

namespace Fugu.Core.IO;

public abstract class Table
{
    public abstract ValueTask ReadAsync(Memory<byte> buffer, long offset);
}