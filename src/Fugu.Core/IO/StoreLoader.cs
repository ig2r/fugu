using System.Buffers;
using System.Data;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.NetworkInformation;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;

namespace Fugu.IO;

public static class StoreLoader
{
    public static async Task LoadFromStorageAsync(IBackingStorage storage)
    {
        var slabs = await storage.GetAllSlabsAsync();
        var segments = new List<Segment>(capacity: slabs.Count);

        foreach (var slab in slabs)
        {
            var segment = await LoadSegmentHeaderAsync(slab);
            segments.Add(segment);
        }

        // TODO: order segments, decide on which ones to load & which ones to skip

        foreach (var segment in segments)
        {
            await foreach (var changeSet in ReadChangeSetsAsync(segment))
            {
                // TODO: push this change set info to index actor
            }
        }
    }

    private static async Task<Segment> LoadSegmentHeaderAsync(ISlab slab)
    {
        if (slab.Length < StorageFormat.SegmentHeaderSize)
        {
            throw new InvalidOperationException("Slab too small to contain segment header.");
        }

        var headerBytes = new byte[StorageFormat.SegmentHeaderSize];
        await slab.ReadAsync(headerBytes, 0);
        return ParseSegmentHeader();

        Segment ParseSegmentHeader()
        {
            var segmentReader = new SegmentReader(new ReadOnlySequence<byte>(headerBytes));
            segmentReader.TryReadSegmentHeader(out var minGeneration, out var maxGeneration);
            return new Segment(minGeneration, maxGeneration, slab);
        }
    }

    private static async IAsyncEnumerable<int> ReadChangeSetsAsync(Segment segment)
    {
        // TODO: create PipeReader for segment contents beyond header
        PipeReader pipeReader = await GetChangeSetPipeReaderAsync(segment.Slab);

        while (true)
        {
            var changeSet = await TryParseChangeSetAsync(pipeReader);

            if (changeSet is null)
            {
                // Marks completion of the input stream
                break;
            }

            // TODO: push change set into IndexActor input channel
        }

        yield break;
    }

    private static async ValueTask<PipeReader> GetChangeSetPipeReaderAsync(ISlab slab)
    {
        // TODO: this implementation reads the entire slab in one go. This won't work if the slab is larger
        // than 4 GB, in which case it'll need to read chunk-by-chunk.
        var buffer = new byte[slab.Length -  StorageFormat.SegmentHeaderSize];
        await slab.ReadAsync(buffer, StorageFormat.SegmentHeaderSize);
        return PipeReader.Create(new ReadOnlySequence<byte>(buffer));
    }

    private static async ValueTask<int?> TryParseChangeSetAsync(PipeReader pipeReader)
    {
        var parseContext = new ChangeSetParseContext();
        ReadResult readResult;

        do
        {
            readResult = await pipeReader.ReadAsync();
            var consumed = ParseChangeSetCore(ref parseContext, readResult);

            // TODO: if parseContext signals that change set has been fully read, then:
            // 1. advance pipeReader to "consumed" position, but do NOT mark rest of buffer as examined;
            // 2. construct WrittenChanges struct (without segment) from parseContext and return that,
            //    thus breaking from the loop.
            if (parseContext.Current == ChangeSetParseToken.Completed)
            {
                pipeReader.AdvanceTo(consumed);
                return null;
            }

            // Otherwise, we need more data in the buffer to make progress. Mark buffer contents as fully examined
            // to tell PipeReader.ReadAsync that we need more data.
            pipeReader.AdvanceTo(consumed, examined: readResult.Buffer.End);
        }
        while (!readResult.IsCompleted);

        // TODO: input stream has completed -- examine parseContext, and if that shows that we were
        // in the middle of parsing a change set, something obviously went wrong & warrants an exception.

        return null;
    }

    private static SequencePosition ParseChangeSetCore(ref ChangeSetParseContext parseContext, ReadResult readResult)
    {
        var segmentReader = new SegmentReader(readResult.Buffer);

        // TODO: look at current parse state, and based on that, try to pull the corresponding structure from
        // segmentReader & update parse state if successful. If unsuccessful, don't update parse state but
        // return segmentReader.Position instead.
        while (parseContext.Current != ChangeSetParseToken.Completed)
        {
            switch (parseContext.Current)
            {
                case ChangeSetParseToken.ChangeSetHeader:
                    {
                        if (!segmentReader.TryReadChangeSetHeader(out var payloadCount, out var tombstoneCount))
                        {
                            return segmentReader.Position;
                        }

                        parseContext.Current = ChangeSetParseToken.Tombstones;
                        parseContext.RemainingPayloads = payloadCount;
                        parseContext.RemainingTombstones = tombstoneCount;
                        parseContext.Tombstones = new List<byte[]>(capacity: tombstoneCount);
                        parseContext.PayloadKeys = new List<byte[]>(capacity: payloadCount);
                        parseContext.PayloadValueLengths = new Queue<int>(capacity: payloadCount);

                        break;
                    }

                case ChangeSetParseToken.Tombstones:
                    {
                        while (parseContext.RemainingTombstones > 0)
                        {
                            if (!segmentReader.TryReadTombstone(out var key))
                            {
                                return segmentReader.Position;
                            }

                            parseContext.Tombstones.Add(key);
                            parseContext.RemainingTombstones--;
                        }

                        parseContext.Current = ChangeSetParseToken.PayloadHeaders;
                        break;
                    }

                case ChangeSetParseToken.PayloadHeaders:
                    {
                        while (parseContext.RemainingPayloads > 0)
                        {
                            if (!segmentReader.TryReadPayloadHeader(out var key, out var valueLength))
                            {
                                return segmentReader.Position;
                            }

                            parseContext.PayloadKeys.Add(key);
                            parseContext.PayloadValueLengths.Enqueue(valueLength);
                            parseContext.RemainingPayloads--;
                        }

                        parseContext.Current = ChangeSetParseToken.PayloadValues;
                        break;
                    }

                case ChangeSetParseToken.PayloadValues:
                    {
                        while (parseContext.PayloadValueLengths.TryPeek(out var valueLength))
                        {
                            if (!segmentReader.TryAdvancePastPayloadValue(valueLength))
                            {
                                return segmentReader.Position;
                            }

                            parseContext.PayloadValueLengths.Dequeue();
                        }

                        parseContext.Current = ChangeSetParseToken.Completed;
                        break;
                    }

                default:
                    throw new InvalidOperationException();
            }
        }

        return segmentReader.Position;
    }

    private struct ChangeSetParseContext
    {
        public ChangeSetParseToken Current { get; set; }
        public int RemainingPayloads { get; set; }
        public int RemainingTombstones { get; set; }
        public List<byte[]> Tombstones { get; set; }
        public List<byte[]> PayloadKeys { get; set; }
        public Queue<int> PayloadValueLengths { get; set; }
    }

    private enum ChangeSetParseToken : byte
    {
        ChangeSetHeader = 0,
        PayloadHeaders,
        Tombstones,
        PayloadValues,
        Completed,
    }
}
