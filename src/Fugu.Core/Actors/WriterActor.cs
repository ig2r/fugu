using Fugu.Channels;
using Fugu.IO;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class WriterActor
{
    private readonly Channel<ChangeSetAllocated> _changeSetAllocatedChannel;
    private readonly Channel<ChangesWritten> _changesWrittenChannel;

    private Segment? _outputSegment = null;
    private PipeWriter? _outputSegmentPipeWriter = null;

    public WriterActor(
        Channel<ChangeSetAllocated> changeSetAllocatedChannel,
        Channel<ChangesWritten> changesWrittenChannel)
    {
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
        _changesWrittenChannel = changesWrittenChannel;
    }

    public async Task RunAsync()
    {
        long offset = 0;

        while (await _changeSetAllocatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _changeSetAllocatedChannel.Reader.ReadAsync();

            if (_outputSegment is null || message.OutputSlab != _outputSegment.Slab)
            {
                // Close out the previous segment, if any
                if (_outputSegmentPipeWriter is not null)
                {
                    await _outputSegmentPipeWriter.CompleteAsync();
                    _outputSegmentPipeWriter = null;
                }

                _outputSegment = new Segment(1, 1, message.OutputSlab);
            }

            // Start new segment
            if (_outputSegmentPipeWriter is null)
            {
                _outputSegmentPipeWriter = PipeWriter.Create(message.OutputSlab.Output);
                offset = 0;

                WriteSegmentHeader(_outputSegment, ref offset);
            }

            var writtenPayloads = WriteChangeSet(message.ChangeSet, ref offset);
            await _outputSegmentPipeWriter.FlushAsync();

            // Propagate changes downstream
            await _changesWrittenChannel.Writer.WriteAsync(
                new ChangesWritten(
                    Clock: message.Clock,
                    OutputSegment: _outputSegment,
                    Payloads: writtenPayloads,
                    Tombstones: message.ChangeSet.Tombstones));
        }

        // TODO: Terminate current output segment, if any

        // Propagate completion
        _changesWrittenChannel.Writer.Complete();
    }

    private void WriteSegmentHeader(Segment segment, ref long offset)
    {
        if (_outputSegmentPipeWriter is null)
        {
            throw new InvalidOperationException();
        }

        var segmentWriter = new SegmentWriter(_outputSegmentPipeWriter);
        segmentWriter.WriteSegmentHeader(segment.MinGeneration, segment.MaxGeneration);
        offset += segmentWriter.BytesWritten;
    }

    private IReadOnlyList<WrittenPayload> WriteChangeSet(ChangeSet changeSet, ref long offset)
    {
        if (_outputSegmentPipeWriter is null)
        {
            throw new InvalidOperationException();
        }

        var segmentWriter = new SegmentWriter(_outputSegmentPipeWriter);
        segmentWriter.WriteChangeSetHeader(changeSet.Payloads.Count, changeSet.Tombstones.Count);

        foreach (var tombstone in changeSet.Tombstones)
        {
            segmentWriter.WriteTombstone(tombstone);
        }

        var payloads = changeSet.Payloads.ToArray();
        var writtenPayloads = new List<WrittenPayload>(payloads.Length);

        foreach (var payload in payloads)
        {
            segmentWriter.WritePayloadHeader(payload.Key, payload.Value.Length);
        } 

        offset += segmentWriter.BytesWritten;

        foreach (var payload in payloads)
        {
            _outputSegmentPipeWriter.Write(payload.Value.Span);

            writtenPayloads.Add(new WrittenPayload(Key: payload.Key, ValueOffset: offset, ValueLength: payload.Value.Length));
            offset += payload.Value.Length;
        }

        return writtenPayloads;
    }
}
