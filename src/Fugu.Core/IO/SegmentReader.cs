using Fugu.Utils;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Resources;
using System.Runtime.InteropServices;

namespace Fugu.IO;

public sealed class SegmentReader
{
    private readonly Segment _segment;

    private SegmentReader(Segment segment)
    {
        _segment = segment;
    }

    public Segment Segment => _segment;

    public static async ValueTask<SegmentReader> CreateAsync(ISlab slab)
    {
        if (slab.Length < StorageFormat.SegmentHeaderSize)
        {
            throw new InvalidOperationException("Slab too small to contain segment header.");
        }

        var headerBytes = new byte[StorageFormat.SegmentHeaderSize];
        await slab.ReadAsync(headerBytes, 0);
        var segment = ReadSegmentHeader(headerBytes, slab);
        return new SegmentReader(segment);
    }

    public async IAsyncEnumerable<ChangeSetCoordinates> ReadChangeSetsAsync()
    {
        var pipeReader = await GetChangeSetsPipeReaderAsync();
        long offset = StorageFormat.SegmentHeaderSize;

        var state = new ParseState
        {
            CurrentToken = ParseToken.Start,
        };

        while (true)
        {
            var readResult = await pipeReader.ReadAsync();
            var offsetBeforeParse = readResult.Buffer.GetOffset(readResult.Buffer.Start);

            var (consumed, examined) = Parse(ref state, readResult.Buffer, offset);

            var offsetAfterParse = readResult.Buffer.GetOffset(consumed);
            offset += (offsetAfterParse - offsetBeforeParse);

            pipeReader.AdvanceTo(consumed, examined);

            if (state.CurrentToken == ParseToken.Done)
            {
                // Yield changes to caller
                var changes = new ChangeSetCoordinates(
                    Payloads: Enumerable.Zip(state.PayloadKeys, state.PayloadValues, KeyValuePair.Create).ToArray(),
                    Tombstones: state.Tombstones);
                
                yield return changes;

                // Reinitialize parser state
                state = new ParseState
                {
                    CurrentToken = ParseToken.Start,
                };
            }

            if (readResult.IsCompleted)
            {
                if (state.CurrentToken != ParseToken.Start)
                {
                    throw new InvalidOperationException("Parsing aborted prematurely.");
                }

                var remainder = readResult.Buffer.Slice(consumed);

                // If the remaining buffer is empty, we consumed everything and must break from the
                // loop. Otherwise, continue processing the remaining data.
                if (remainder.IsEmpty)
                {
                    break;
                }
            }
        }

        yield break;
    }

    private static Segment ReadSegmentHeader(byte[] headerBytes, ISlab slab)
    {
        var spanReader = new SpanReader(headerBytes);

        var magic = spanReader.ReadInt64();
        var versionMajor = spanReader.ReadInt16();
        var versionMinor = spanReader.ReadInt16();

        if (magic != 0xDEADBEEF)
        {
            throw new InvalidOperationException("Invalid format.");
        }

        if (versionMajor != 1 || versionMinor != 0)
        {
            throw new InvalidOperationException("Unsupported format version.");
        }

        var minGeneration = spanReader.ReadInt64();
        var maxGeneration = spanReader.ReadInt64();

        return new Segment(minGeneration, maxGeneration, slab);
    }

    private async ValueTask<PipeReader> GetChangeSetsPipeReaderAsync()
    {
        // TODO: this implementation reads the entire slab in one go. This won't work if the slab is larger
        // than 4 GB, in which case it'll need to read chunk-by-chunk.
        var buffer = new byte[Segment.Slab.Length - StorageFormat.SegmentHeaderSize];
        await Segment.Slab.ReadAsync(buffer, StorageFormat.SegmentHeaderSize);
        return PipeReader.Create(new ReadOnlySequence<byte>(buffer));
    }

    private (SequencePosition Consumed, SequencePosition Examined) Parse(ref ParseState state, ReadOnlySequence<byte> buffer, long offset)
    {
        var sequenceReader = new SequenceReader<byte>(buffer);

        while (true)
        {
            switch (state.CurrentToken)
            {
                case ParseToken.Start:
                    {
                        var position = sequenceReader.Position;

                        if (sequenceReader.TryReadLittleEndian(out int payloadCount) &&
                            sequenceReader.TryReadLittleEndian(out int tombstoneCount))
                        {
                            state.PayloadKeyLengths = new int[payloadCount];
                            state.TombstoneKeyLengths = new int[tombstoneCount];
                            state.PayloadValueLengths = new int[payloadCount];

                            state.PayloadKeys = new List<byte[]>(capacity: payloadCount);
                            state.PayloadValues = new List<SlabSubrange>(capacity: payloadCount);
                            state.Tombstones = new List<byte[]>(capacity: tombstoneCount);

                            state.CurrentToken = ParseToken.PayloadKeyLengths;
                            break;
                        }

                        // Not enough data left in sequence
                        return (position, buffer.End);
                    }

                case ParseToken.PayloadKeyLengths:
                    {
                        var lengthBytes = MemoryMarshal.AsBytes<int>(state.PayloadKeyLengths);

                        if (!sequenceReader.TryCopyTo(lengthBytes))
                        {
                            return (sequenceReader.Position, buffer.End);
                        }

                        sequenceReader.Advance(lengthBytes.Length);

                        if (!BitConverter.IsLittleEndian)
                        {
                            BinaryPrimitives.ReverseEndianness(state.PayloadKeyLengths, state.PayloadKeyLengths);
                        }

                        state.CurrentToken = ParseToken.TombstoneKeyLengths;
                        break;
                    }

                case ParseToken.TombstoneKeyLengths:
                    {
                        var lengthBytes = MemoryMarshal.AsBytes<int>(state.TombstoneKeyLengths);

                        if (!sequenceReader.TryCopyTo(lengthBytes))
                        {
                            return (sequenceReader.Position, buffer.End);
                        }

                        sequenceReader.Advance(lengthBytes.Length);

                        if (!BitConverter.IsLittleEndian)
                        {
                            BinaryPrimitives.ReverseEndianness(state.TombstoneKeyLengths, state.TombstoneKeyLengths);
                        }

                        state.CurrentToken = ParseToken.PayloadValueLengths;
                        break;
                    }

                case ParseToken.PayloadValueLengths:
                    {
                        var lengthBytes = MemoryMarshal.AsBytes<int>(state.PayloadValueLengths);

                        if (!sequenceReader.TryCopyTo(lengthBytes))
                        {
                            return (sequenceReader.Position, buffer.End);
                        }

                        sequenceReader.Advance(lengthBytes.Length);

                        if (!BitConverter.IsLittleEndian)
                        {
                            BinaryPrimitives.ReverseEndianness(state.PayloadValueLengths, state.PayloadValueLengths);
                        }

                        state.CurrentToken = ParseToken.Keys;
                        break;
                    }

                case ParseToken.Keys:
                    {
                        var payloadKeysSize = state.PayloadKeyLengths.Sum();
                        var tombstonesSize = state.TombstoneKeyLengths.Sum();

                        if (sequenceReader.Remaining < payloadKeysSize + tombstonesSize)
                        {
                            return (sequenceReader.Position, buffer.End);
                        }

                        // Read & unpack payload keys
                        foreach (var l in state.PayloadKeyLengths)
                        {
                            var key = new byte[l];
                            sequenceReader.TryCopyTo(key);
                            sequenceReader.Advance(l);

                            state.PayloadKeys.Add(key);
                        }

                        // Read & unpack tombstones
                        foreach (var l in state.TombstoneKeyLengths)
                        {
                            var key = new byte[l];
                            sequenceReader.TryCopyTo(key);
                            sequenceReader.Advance(l);

                            state.Tombstones.Add(key);
                        }

                        state.CurrentToken = ParseToken.Values;
                        break;
                    }

                case ParseToken.Values:
                    {
                        var valueLengthSum = state.PayloadValueLengths.Sum();
                        if (sequenceReader.Remaining < valueLengthSum)
                        {
                            return (sequenceReader.Position, buffer.End);
                        }

                        foreach (var valueLength in state.PayloadValueLengths)
                        {
                            state.PayloadValues.Add(
                                new SlabSubrange
                                {
                                    Offset = offset + sequenceReader.Consumed,
                                    Length = valueLength,
                                });

                            sequenceReader.Advance(valueLength);
                        }

                        state.CurrentToken = ParseToken.Checksum;
                        break;
                    }

                case ParseToken.Checksum:
                    {
                        state.CurrentToken = ParseToken.Done;
                        break;
                    }

                case ParseToken.Done:
                    {
                        return (sequenceReader.Position, sequenceReader.Position);
                    }

                default:
                    throw new InvalidOperationException();
            }
        }
    }

    private enum ParseToken
    {
        Start = 1,
        PayloadKeyLengths,
        TombstoneKeyLengths,
        PayloadValueLengths,
        Keys,
        Values,
        Checksum,
        Done,
    }

    private struct ParseState
    {
        public ParseToken CurrentToken { get; set; }

        public int[] PayloadKeyLengths { get; set; }
        public int[] TombstoneKeyLengths { get; set; }
        public int[] PayloadValueLengths { get; set; }

        // These will be used to construct the ChangeSetCoordinates return value:
        public List<byte[]> PayloadKeys { get; set; }
        public List<SlabSubrange> PayloadValues { get; set; }
        public List<byte[]> Tombstones { get; set; }
    }
}
