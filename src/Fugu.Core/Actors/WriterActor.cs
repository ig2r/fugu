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
        while (await _changeSetAllocatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _changeSetAllocatedChannel.Reader.ReadAsync();

            if (_outputSegment is null || message.OutputSlab != _outputSegment.Slab)
            {
                if (_outputSegmentPipeWriter is not null)
                {
                    await _outputSegmentPipeWriter.CompleteAsync();
                    _outputSegmentPipeWriter = null;
                }

                _outputSegment = new Segment(0, 0, message.OutputSlab);
            }

            if (_outputSegmentPipeWriter is null)
            {
                _outputSegmentPipeWriter = PipeWriter.Create(message.OutputSlab.Output);
                WriteSegmentHeader(_outputSegment);
            }

            WriteChangeSet(message.ChangeSet);
            await _outputSegmentPipeWriter.FlushAsync();

            await _changesWrittenChannel.Writer.WriteAsync(
                new ChangesWritten(
                    Clock: message.Clock,
                    OutputSegment: _outputSegment));
        }
    }

    private void WriteSegmentHeader(Segment segment)
    {
        if (_outputSegmentPipeWriter is null)
        {
            throw new InvalidOperationException();
        }

        var segmentWriter = new SegmentWriter(_outputSegmentPipeWriter);
        segmentWriter.WriteSegmentHeader(segment.MinGeneration, segment.MaxGeneration);
    }

    private void WriteChangeSet(ChangeSet changeSet)
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

        var payloadValues = new List<ReadOnlyMemory<byte>>(changeSet.Payloads.Count);

        foreach (var payload in changeSet.Payloads)
        {
            segmentWriter.WritePayloadHeader(payload.Key, payload.Value.Length);
            payloadValues.Add(payload.Value);
        }

        foreach (var payloadValue in payloadValues)
        {
            _outputSegmentPipeWriter.Write(payloadValue.Span);
        }
    }
}
