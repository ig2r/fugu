namespace Fugu.Core.IO;

public class InMemoryTable : WritableTable
{
    private readonly MemoryStream _stream;

    public InMemoryTable(int capacity)
    {
        _stream = new MemoryStream(capacity);
    }

    public override Stream OutputStream => _stream;

    public override ValueTask ReadAsync(Memory<byte> buffer, long offset)
    {
        var contents = _stream.ToArray();
        contents.AsMemory((int)offset, buffer.Length).CopyTo(buffer);
        return ValueTask.CompletedTask;
    }
}
