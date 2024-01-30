using Fugu.Channels;
using Fugu.IO;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class WriterActor
{
    private readonly ChannelReader<ChangeSetAllocated> _changeSetAllocatedChannelReader;
    private readonly ChannelWriter<ChangesWritten> _changesWrittenChannelWriter;

    private long _outputGeneration;
    private SegmentBuilder? _segmentBuilder;

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

            if (_segmentBuilder is null || message.OutputSlab != _segmentBuilder.Segment.Slab)
            {
                // Close out the previous segment, if any
                if (_segmentBuilder is not null)
                {
                    await _segmentBuilder.CompleteAsync();
                    _segmentBuilder = null;
                }

                _outputGeneration++;
            }

            // Start new segment if needed
            _segmentBuilder ??= await SegmentBuilder.CreateAsync(message.OutputSlab, _outputGeneration, _outputGeneration);

            var writtenPayloads = await _segmentBuilder.WriteChangeSetAsync(message.ChangeSet);

            // Propagate changes downstream
            await _changesWrittenChannelWriter.WriteAsync(
                new ChangesWritten(
                    Clock: message.Clock,
                    OutputSegment: _segmentBuilder.Segment,
                    Payloads: writtenPayloads,
                    Tombstones: message.ChangeSet.Tombstones));
        }

        // TODO: Terminate current output segment, if any

        // Propagate completion
        _changesWrittenChannelWriter.Complete();
    }
}
