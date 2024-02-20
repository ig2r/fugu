using Fugu.Utils;
using System.Buffers;
using System.IO.Hashing;
using System.IO.Pipelines;

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

        var state = new SegmentChangeSetsParser.ParseState
        {
            CurrentToken = SegmentChangeSetsParser.ParseToken.Start,
            Hash64 = new XxHash64(_segment.MinGeneration),
        };

        while (true)
        {
            var readResult = await pipeReader.ReadAsync();
            var offsetBeforeParse = readResult.Buffer.GetOffset(readResult.Buffer.Start);

            var (consumed, examined) = SegmentChangeSetsParser.Parse(ref state, readResult.Buffer, offset);

            var offsetAfterParse = readResult.Buffer.GetOffset(consumed);
            offset += (offsetAfterParse - offsetBeforeParse);

            pipeReader.AdvanceTo(consumed, examined);

            if (state.CurrentToken == SegmentChangeSetsParser.ParseToken.FailedChecksum)
            {
                break;
            }

            if (state.CurrentToken == SegmentChangeSetsParser.ParseToken.Done)
            {
                // Yield changes to caller
                var changes = new ChangeSetCoordinates(
                    Payloads: Enumerable.Zip(state.PayloadKeys, state.PayloadValues, KeyValuePair.Create).ToArray(),
                    Tombstones: state.Tombstones);
                
                yield return changes;

                // Reinitialize parser state
                state = new SegmentChangeSetsParser.ParseState
                {
                    CurrentToken = SegmentChangeSetsParser.ParseToken.Start,
                    Hash64 = state.Hash64,
                };
            }

            if (readResult.IsCompleted)
            {
                if (state.CurrentToken != SegmentChangeSetsParser.ParseToken.Start)
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
}
