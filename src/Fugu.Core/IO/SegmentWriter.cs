using Fugu.Utils;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace Fugu.IO;

public sealed class SegmentWriter
{
    private readonly Segment _segment;
    private readonly PipeWriter _pipeWriter;
    private readonly XxHash64 _hash64;
    private long _offset;

    private SegmentWriter(Segment segment, PipeWriter pipeWriter, long initialOffset)
    {
        _segment = segment;
        _pipeWriter = pipeWriter;
        _offset = initialOffset;

        _hash64 = new XxHash64(_segment.MinGeneration);
    }

    public Segment Segment => _segment;

    public static async ValueTask<SegmentWriter> CreateAsync(IWritableSlab outputSlab, long minGeneration, long maxGeneration)
    {
        var segment = new Segment(minGeneration, maxGeneration, outputSlab);
        var pipeWriter = PipeWriter.Create(outputSlab.Output);
        var initialOffset = WriteSegmentHeader(pipeWriter, minGeneration, maxGeneration);
        await pipeWriter.FlushAsync();

        return new SegmentWriter(segment, pipeWriter, initialOffset);
    }

    public ValueTask CompleteAsync()
    {
        return _pipeWriter.CompleteAsync();
    }

    public async ValueTask<ChangeSetCoordinates> WriteChangeSetAsync(ChangeSet changeSet)
    {
        var changes = WriteChangeSet(changeSet);
        await _pipeWriter.FlushAsync();
        return changes;
    }

    private static long WriteSegmentHeader(IBufferWriter<byte> bufferWriter, long minGeneration, long maxGeneration)
    {
        var span = bufferWriter.GetSpan(StorageFormat.SegmentHeaderSize);
        var writer = new SpanWriter(span);

        writer.WriteInt64(0xDEADBEEF);              // Magic
        writer.WriteInt16(1);                       // Format version, major
        writer.WriteInt16(0);                       // Format version, minor
        writer.WriteInt64(minGeneration);
        writer.WriteInt64(maxGeneration);

        bufferWriter.Advance(StorageFormat.SegmentHeaderSize);
        return StorageFormat.SegmentHeaderSize;
    }

    private ChangeSetCoordinates WriteChangeSet(ChangeSet changeSet)
    {
        var payloads = changeSet.Payloads.ToArray();
        var tombstones = changeSet.Tombstones.ToArray();

        var headerLength = Unsafe.SizeOf<int>() * (2 + (2 * changeSet.Payloads.Count) + changeSet.Tombstones.Count);

        // The following code relies on PipeWriter implementing IBufferWriter<byte>
        {
            var span = _pipeWriter.GetSpan(headerLength);

            var spanWriter = new SpanWriter(span);
            spanWriter.WriteInt32(changeSet.Payloads.Count);
            spanWriter.WriteInt32(changeSet.Tombstones.Count);

            var payloadKeyLengths = payloads.Select(kvp => kvp.Key.Length).ToArray();
            var tombstoneKeyLengths = tombstones.Select(tombstone => tombstone.Length).ToArray();
            var payloadValueLengths = payloads.Select(kvp => kvp.Value.Length).ToArray();

            spanWriter.WriteInt32Array(payloadKeyLengths);
            spanWriter.WriteInt32Array(tombstoneKeyLengths);
            spanWriter.WriteInt32Array(payloadValueLengths);

            _pipeWriter.Advance(headerLength);
            _offset += headerLength;
        }

        var writtenPayloads = new List<KeyValuePair<byte[], SlabSubrange>>(capacity: payloads.Length);

        // Write keys
        foreach (var payload in payloads)
        {
            _pipeWriter.Write(payload.Key);
            _hash64.Append(payload.Key);

            _offset += payload.Key.Length;
        }

        foreach (var tombstone in tombstones)
        {
            _pipeWriter.Write(tombstone);
            _hash64.Append(tombstone);

            _offset += tombstone.Length;
        }

        // Write payload values
        foreach (var payload in payloads)
        {
            _pipeWriter.Write(payload.Value.Span);
            _hash64.Append(payload.Value.Span);

            writtenPayloads.Add(KeyValuePair.Create(payload.Key, new SlabSubrange(_offset, payload.Value.Length)));
            _offset += payload.Value.Length;
        }

        // Write actual checksum
        var checksum = _hash64.GetCurrentHashAsUInt64();
        Span<byte> checksumBytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(checksumBytes, checksum);

        _pipeWriter.Write(checksumBytes);
        _offset += checksumBytes.Length;

        return new ChangeSetCoordinates(writtenPayloads, tombstones);
    }
}
