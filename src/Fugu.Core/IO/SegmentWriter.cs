using System.Buffers;

namespace Fugu.IO;

public ref struct SegmentWriter
{
    private readonly IBufferWriter<byte> _bufferWriter;

    public SegmentWriter(IBufferWriter<byte> bufferWriter)
    {
        _bufferWriter = bufferWriter;
    }

    public long BytesWritten { get; private set; }

    public void WriteSegmentHeader(long minGeneration, long maxGeneration)
    {
        // TODO: Instead of acquiring a small chunk of memory from _bufferWriter in each call,
        // ask for a bigger chunk and store it in a field, directing multiple writes into it.
        // Downside is that we'll need to track how much data was already written and call
        // Advance() when space in the current segment runs out, OR when done writing to the
        // SegmentFormatter.
        var span = _bufferWriter.GetSpan(StorageFormat.SegmentHeaderSize);
        var writer = new SpanWriter(span);

        writer.WriteInt64(0xDEADBEEF);              // Magic
        writer.WriteInt16(1);                       // Format version, major
        writer.WriteInt16(0);                       // Format version, minor
        writer.WriteInt64(minGeneration);
        writer.WriteInt64(maxGeneration);

        _bufferWriter.Advance(StorageFormat.SegmentHeaderSize);
        BytesWritten += StorageFormat.SegmentHeaderSize;
    }

    public void WriteChangeSetHeader(int payloadCount, int tombstoneCount)
    {
        var span = _bufferWriter.GetSpan(StorageFormat.ChangeSetHeaderSize);
        var writer = new SpanWriter(span);

        writer.WriteByte(1);
        writer.WriteInt32(payloadCount);
        writer.WriteInt32(tombstoneCount);

        _bufferWriter.Advance(StorageFormat.ChangeSetHeaderSize);
        BytesWritten += StorageFormat.ChangeSetHeaderSize;
    }

    public void WritePayloadHeader(ReadOnlySpan<byte> key, int valueLength)
    {
        var span = _bufferWriter.GetSpan(StorageFormat.PayloadHeaderPrefixSize);
        var writer = new SpanWriter(span);

        writer.WriteInt32(key.Length);
        writer.WriteInt32(valueLength);
        _bufferWriter.Advance(StorageFormat.PayloadHeaderPrefixSize);
        BytesWritten += StorageFormat.PayloadHeaderPrefixSize;

        _bufferWriter.Write(key);
        BytesWritten += key.Length;
    }

    public void WriteTombstone(ReadOnlySpan<byte> key)
    {
        var span = _bufferWriter.GetSpan(StorageFormat.TombstonePrefixSize);
        var writer = new SpanWriter(span);

        writer.WriteInt32(key.Length);
        _bufferWriter.Advance(StorageFormat.TombstonePrefixSize);
        BytesWritten += StorageFormat.TombstonePrefixSize;

        _bufferWriter.Write(key);
        BytesWritten += key.Length;
    }
}
