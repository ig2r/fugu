using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Fugu.IO;

/// <summary>
/// Read-path dual to <see cref="SegmentWriter"/>.
/// </summary>
public ref struct SegmentReader
{
    private SequenceReader<byte> _sequenceReader;

    public SegmentReader(ReadOnlySequence<byte> sequence)
    {
        _sequenceReader = new SequenceReader<byte>(sequence);
    }

    public readonly bool End => _sequenceReader.End;
    public readonly SequencePosition Position => _sequenceReader.Position;

    public bool TryReadSegmentHeader(out long minGeneration, out long maxGeneration)
    {
        if (!_sequenceReader.TryReadExact(StorageFormat.SegmentHeaderSize, out var token))
        {
            minGeneration = default;
            maxGeneration = default;
            return false;
        }

        var header = token.IsSingleSegment
            ? token.FirstSpan
            : token.ToArray();      // TODO: allocate on stack or copy to pooled array instead

        var spanReader = new SpanReader(header);

        spanReader.ReadInt64();     // Magic
        spanReader.ReadInt16();     // Major version
        spanReader.ReadInt16();     // Minor version

        minGeneration = spanReader.ReadInt64();
        maxGeneration = spanReader.ReadInt64();

        return true;
    }

    public bool TryReadChangeSetHeader(out int payloadCount, out int tombstoneCount)
    {
        if (!_sequenceReader.TryReadExact(StorageFormat.ChangeSetHeaderSize, out var token))
        {
            payloadCount = default;
            tombstoneCount = default;
            return false;
        }

        var header = token.IsSingleSegment
            ? token.FirstSpan
            : token.ToArray();

        var spanReader = new SpanReader(header);

        var tag = spanReader.ReadByte();
        payloadCount = spanReader.ReadInt32();
        tombstoneCount = spanReader.ReadInt32();

        if (tag != 1)
        {
            throw new InvalidDataException();
        }

        return true;
    }

    public bool TryReadPayloadHeader([NotNullWhen(true)] out byte[]? key, out int valueLength)
    {
        if (!_sequenceReader.TryReadExact(StorageFormat.PayloadHeaderPrefixSize, out var token))
        {
            key = null;
            valueLength = default;
            return false;
        }

        var header = token.IsSingleSegment
            ? token.FirstSpan
            : token.ToArray();

        var spanReader = new SpanReader(header);

        var keyLength = spanReader.ReadInt32();
        valueLength = spanReader.ReadInt32();

        if (_sequenceReader.Remaining < keyLength)
        {
            key = null;
            _sequenceReader.Rewind(StorageFormat.PayloadHeaderPrefixSize);
            return false;
        }

        _sequenceReader.TryReadExact(keyLength, out var keySequence);
        key = keySequence.ToArray();

        return true;
    }

    public bool TryReadTombstone([NotNullWhen(true)] out byte[]? key)
    {
        if (!_sequenceReader.TryReadExact(StorageFormat.TombstonePrefixSize, out var token))
        {
            key = null;
            return false;
        }

        var header = token.IsSingleSegment
            ? token.FirstSpan
            : token.ToArray();

        var spanReader = new SpanReader(header);

        var keyLength = spanReader.ReadInt32();

        if (_sequenceReader.Remaining < keyLength)
        {
            key = null;
            _sequenceReader.Rewind(StorageFormat.TombstonePrefixSize);
            return false;
        }

        _sequenceReader.TryReadExact(keyLength, out var keySequence);
        key = keySequence.ToArray();

        return true;
    }

    public bool TryAdvancePastPayloadValue(int valueLength)
    {
        if (_sequenceReader.Remaining < valueLength)
        {
            return false;
        }

        _sequenceReader.Advance(valueLength);
        return true;
    }
}
