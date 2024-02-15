using Fugu.Channels;
using Fugu.IO;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class WriterActor
{
    private readonly ChannelReader<ChangeSetAllocated> _changeSetAllocatedChannelReader;
    private readonly ChannelWriter<ChangesWritten> _changesWrittenChannelWriter;

    private long _outputGeneration;
    private SegmentWriter? _segmentWriter;

    public WriterActor(
        ChannelReader<ChangeSetAllocated> changeSetAllocatedChannelReader,
        ChannelWriter<ChangesWritten> changesWrittenChannelWriter,
        long maxGeneration)
    {
        _changeSetAllocatedChannelReader = changeSetAllocatedChannelReader;
        _changesWrittenChannelWriter = changesWrittenChannelWriter;
        _outputGeneration = maxGeneration;
    }

    public async Task RunAsync()
    {
        while (await _changeSetAllocatedChannelReader.WaitToReadAsync())
        {
            var message = await _changeSetAllocatedChannelReader.ReadAsync();

            if (_segmentWriter is null || message.OutputSlab != _segmentWriter.Segment.Slab)
            {
                // Close out the previous segment, if any
                await CloseOutputSegmentAsync();
                _outputGeneration++;
            }

            // Start new segment if needed
            _segmentWriter ??= await SegmentWriter.CreateAsync(message.OutputSlab, _outputGeneration, _outputGeneration);

            var coordinates = await _segmentWriter.WriteChangeSetAsync(message.ChangeSet);

            // Propagate changes downstream
            await _changesWrittenChannelWriter.WriteAsync(
                new ChangesWritten(
                    Clock: message.Clock,
                    OutputSegment: _segmentWriter.Segment,
                    Payloads: coordinates.Payloads,
                    Tombstones: coordinates.Tombstones));
        }

        // Terminate current output segment, if any
        await CloseOutputSegmentAsync();

        // Propagate completion
        _changesWrittenChannelWriter.Complete();
    }

    private ValueTask CloseOutputSegmentAsync()
    {
        if (_segmentWriter is null)
        {
            return ValueTask.CompletedTask;
        }

        var writer = _segmentWriter;
        _segmentWriter = null;

        return writer.CompleteAsync();
    }
}
