using Fugu.Utils;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Fugu.IO;

public static class SegmentChangeSetsParser
{
    public static (SequencePosition Consumed, SequencePosition Examined) Parse(
        ref ParseState state,
        ReadOnlySequence<byte> buffer,
        long offset)
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

    public enum ParseToken
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

    public struct ParseState
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
