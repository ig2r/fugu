using System.Buffers;

namespace Fugu.Storage;

public ref struct SegmentWriter
{
    private readonly IBufferWriter<byte> _bufferWriter;

    public SegmentWriter(IBufferWriter<byte> bufferWriter)
    {
        _bufferWriter = bufferWriter;
    }

    public void WriteSegmentHeader(long minGeneration, long maxGeneration)
    {
        const int size = 8 + 2 + 2 + 8 + 8;

        // TODO: Instead of acquiring a small chunk of memory from _bufferWriter in each call,
        // ask for a bigger chunk and store it in a field, directing multiple writes into it.
        // Downside is that we'll need to track how much data was already written and call
        // Advance() when space in the current segment runs out, OR when done writing to the
        // SegmentFormatter.
        var span = _bufferWriter.GetSpan(size);
        var writer = new SpanWriter(span);

        writer.WriteInt64(0xDEADBEEF);              // Magic
        writer.WriteInt16(1);                       // Format version, major
        writer.WriteInt16(0);                       // Format version, minor
        writer.WriteInt64(minGeneration);
        writer.WriteInt64(maxGeneration);

        _bufferWriter.Advance(size);
    }

    public void WriteChangeSetHeader(int payloadCount, int tombstoneCount)
    {
        const int size = 1 + 4 + 4;

        var span = _bufferWriter.GetSpan(size);
        var writer = new SpanWriter(span);

        writer.WriteByte(1);
        writer.WriteInt32(payloadCount);
        writer.WriteInt32(tombstoneCount);

        _bufferWriter.Advance(size);
    }

    public void WritePayloadHeader(ReadOnlySpan<byte> key, int valueLength)
    {
        const int size = 4 + 4;

        var span = _bufferWriter.GetSpan(size);
        var writer = new SpanWriter(span);

        writer.WriteInt32(key.Length);
        writer.WriteInt32(valueLength);
        _bufferWriter.Advance(size);

        _bufferWriter.Write(key);
    }

    public void WriteTombstone(ReadOnlySpan<byte> key)
    {
        const int size = 4;

        var span = _bufferWriter.GetSpan(size);
        var writer = new SpanWriter(span);

        writer.WriteInt32(key.Length);
        _bufferWriter.Advance(size);

        _bufferWriter.Write(key);
    }
}
