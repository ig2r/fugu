using System.Buffers;

namespace Fugu.Core.IO;

public abstract class Table : ReadOnlyTable
{
    public abstract IBufferWriter<byte> BufferWriter { get; }
}
