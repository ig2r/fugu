using Fugu.Utils;
using System.Buffers;
using System.IO.Pipelines;

namespace Fugu.IO;

/// <summary>
/// Provides a high level interface to create a segment from a series of individual ChangeSets.
/// </summary>
public sealed class SegmentBuilder
{
    private readonly PipeWriter _pipeWriter;
    private long _offset;

    private SegmentBuilder(Segment segment, PipeWriter pipeWriter, long initialOffset)
    {
        Segment = segment;
        _pipeWriter = pipeWriter;
        _offset = initialOffset;
    }

    public Segment Segment { get; }

    public static async ValueTask<SegmentBuilder> CreateAsync(IWritableSlab outputSlab, long minGeneration, long maxGeneration)
    {
        var segment = new Segment(minGeneration, maxGeneration, outputSlab);
        var pipeWriter = PipeWriter.Create(outputSlab.Output);
        var initialOffset = WriteSegmentHeader();
        await pipeWriter.FlushAsync();

        return new SegmentBuilder(segment, pipeWriter, initialOffset);

        long WriteSegmentHeader()
        {
			var segmentWriter = new SegmentWriter(pipeWriter);
			segmentWriter.WriteSegmentHeader(segment.MinGeneration, segment.MaxGeneration);
            return segmentWriter.BytesWritten;
		}
	}

    public ValueTask CompleteAsync()
    {
        return _pipeWriter.CompleteAsync();
    }

    public async ValueTask<IReadOnlyList<KeyValuePair<byte[], SlabSubrange>>> WriteChangeSetAsync(ChangeSet changeSet)
    {
        var changesWritten = WriteChangeSet(changeSet);
        await _pipeWriter.FlushAsync();
        return changesWritten;
    }

    private List<KeyValuePair<byte[], SlabSubrange>> WriteChangeSet(ChangeSet changeSet)
    {
        var segmentWriter = new SegmentWriter(_pipeWriter);
        segmentWriter.WriteChangeSetHeader(changeSet.Payloads.Count, changeSet.Tombstones.Count);

        foreach (var tombstone in changeSet.Tombstones)
        {
            segmentWriter.WriteTombstone(tombstone);
        }

        var payloads = changeSet.Payloads.ToArray();
        var writtenPayloads = new List<KeyValuePair<byte[], SlabSubrange>>(payloads.Length);

        foreach (var payload in payloads)
        {
            segmentWriter.WritePayloadHeader(payload.Key, payload.Value.Length);
        }

        _offset += segmentWriter.BytesWritten;

        foreach (var payload in payloads)
        {
            _pipeWriter.Write(payload.Value.Span);

            writtenPayloads.Add(new(payload.Key, new(_offset, payload.Value.Length)));
            _offset += payload.Value.Length;
        }

        return writtenPayloads;
    }
}
