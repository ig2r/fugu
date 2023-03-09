using System.IO.Pipelines;

namespace Fugu.Core.IO;

public class InMemoryTable : WritableTable
{
    private readonly MemoryStream _stream;
    private readonly PipeWriter _writer;

    public InMemoryTable(int capacity)
    {
        _stream = new MemoryStream(capacity);
        _writer = PipeWriter.Create(_stream);
    }

    public override PipeWriter Writer => _writer;

    public override ValueTask ReadAsync(Memory<byte> buffer, long offset)
    {
        var contents = _stream.ToArray();
        contents.AsMemory((int)offset, buffer.Length).CopyTo(buffer);
        return ValueTask.CompletedTask;
    }
}
