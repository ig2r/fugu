using Fugu.Channels;
using Fugu.Utils;
using System.Buffers;
using System.Collections.Immutable;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Fugu.IO;

public static class StoreLoader
{
    public static async Task LoadFromStorageAsync(IBackingStorage storage, Channel<ChangesWritten> changesWrittenChannel)
    {
        var slabs = await storage.GetAllSlabsAsync();
        var segments = new List<Segment>(capacity: slabs.Count);

        foreach (var slab in slabs)
        {
            var segment = await LoadSegmentHeaderAsync(slab);
            segments.Add(segment);
        }

        // Order segments, decide on which ones to load & which ones to skip
        // TODO: The current implementation assumes that generations will never overlap, hence comparing MinGeneration
        // is sufficient to establish proper ordering. This assumption will NO LONGER BE VALID once we implement compaction.
        segments.Sort((x, y) => Comparer<long>.Default.Compare(x.MinGeneration, y.MinGeneration));

        // For all change sets across all segments in order, feed these change sets to index actor
        foreach (var segment in segments)
        {
            await foreach (var changeSet in ReadChangeSetsAsync(segment))
            {
                await changesWrittenChannel.Writer.WriteAsync(changeSet);
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

    // This level knows the segment, constructs the PipeReader, and tracks authoritative offset in the
    // segment while reading. Therefore, its job is to yield a sequence of channel messages that can
    // be readily submitted to the index actor.
    private static async IAsyncEnumerable<ChangesWritten> ReadChangeSetsAsync(Segment segment)
    {
        // Create PipeReader for segment contents beyond header
        PipeReader pipeReader = await GetChangeSetPipeReaderAsync(segment.Slab);
        long offset = StorageFormat.SegmentHeaderSize;

        while (true)
        {
            var parseContext = new ChangeSetParseContext();
            ReadResult readResult;

            do
            {
                readResult = await pipeReader.ReadAsync();
                var consumed = ParseChangeSetCore(ref parseContext, offset, readResult);
                offset += readResult.Buffer.GetOffset(consumed);

                // TODO: if parseContext signals that change set has been fully read, then:
                // 1. advance pipeReader to "consumed" position, but do NOT mark rest of buffer as examined;
                // 2. construct WrittenChanges struct (without segment) from parseContext and return that,
                //    thus breaking from the loop.
                if (parseContext.Current == ChangeSetParseToken.Completed)
                {
                    pipeReader.AdvanceTo(consumed);
                    break;
                }

                // Otherwise, we need more data in the buffer to make progress. Mark buffer contents as fully examined
                // to tell PipeReader.ReadAsync that we need more data.
                pipeReader.AdvanceTo(consumed, examined: readResult.Buffer.End);
            }
            while (!readResult.IsCompleted);

            if (parseContext.Current == ChangeSetParseToken.ChangeSetHeader)
            {
                // Parsing failed to even read a change set header? We're done here.
                break;
            }
            else if (parseContext.Current == ChangeSetParseToken.Completed)
            {
                // Parsing completed a full change set? Return it.
                var payloads = Enumerable.Zip(
                    parseContext.PayloadKeys,
                    parseContext.PayloadValues,
                    (k, v) => new KeyValuePair<byte[], SlabSubrange>(k, v)).ToArray();

                yield return new ChangesWritten(
                    new VectorClock(0, 0),
                    segment,
                    payloads,
                    parseContext.Tombstones.ToImmutableHashSet());
            }
            else
            {
                // Otherwise, parsing aborted in the middle of a change set. Something went wrong.
                throw new InvalidOperationException("Reading aborted before change set was completed.");
            }
        }
    }

    private static async ValueTask<PipeReader> GetChangeSetPipeReaderAsync(ISlab slab)
    {
        // TODO: this implementation reads the entire slab in one go. This won't work if the slab is larger
        // than 4 GB, in which case it'll need to read chunk-by-chunk.
        var buffer = new byte[slab.Length - StorageFormat.SegmentHeaderSize];
        await slab.ReadAsync(buffer, StorageFormat.SegmentHeaderSize);
        return PipeReader.Create(new ReadOnlySequence<byte>(buffer));
    }

    private static SequencePosition ParseChangeSetCore(ref ChangeSetParseContext parseContext, long offset, ReadResult readResult)
    {
        var segmentReader = new SegmentReader(readResult.Buffer);

        // Based on current parse state, try to pull the corresponding structure from segmentReader & update parse state if
        // successful. If unsuccessful, don't update parse state but return segmentReader.Position instead to signal that
        // we need more data to proceed.
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
                        parseContext.PayloadValues = new List<SlabSubrange>(capacity: payloadCount);
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
                            var valueOffset = offset + segmentReader.Consumed;

                            if (!segmentReader.TryAdvancePastPayloadValue(valueLength))
                            {
                                return segmentReader.Position;
                            }

                            parseContext.PayloadValueLengths.Dequeue();

                            var payloadValue = new SlabSubrange(valueOffset, valueLength);
                            parseContext.PayloadValues.Add(payloadValue);
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
        public ChangeSetParseToken Current;
        public int RemainingPayloads;
        public int RemainingTombstones;
        public List<byte[]> Tombstones;
        public List<byte[]> PayloadKeys;
        public List<SlabSubrange> PayloadValues;
        public Queue<int> PayloadValueLengths;
    }

    private enum ChangeSetParseToken : byte
    {
        ChangeSetHeader = default,
        PayloadHeaders,
        Tombstones,
        PayloadValues,
        Completed,
    }
}
