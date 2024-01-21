using Fugu.Channels;
using Fugu.IO;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class WriterActor
{
    private readonly Channel<ChangeSetAllocated> _changeSetAllocatedChannel;
    private readonly Channel<ChangesWritten> _changesWrittenChannel;

    private long _outputGeneration;
    private SegmentBuilder? _segmentBuilder;

    public WriterActor(
        Channel<ChangeSetAllocated> changeSetAllocatedChannel,
        Channel<ChangesWritten> changesWrittenChannel,
        long maxGeneration)
    {
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
        _changesWrittenChannel = changesWrittenChannel;
        _outputGeneration = maxGeneration;
    }

    public async Task RunAsync()
    {
        while (await _changeSetAllocatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _changeSetAllocatedChannel.Reader.ReadAsync();

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
            _segmentBuilder ??= SegmentBuilder.Create(message.OutputSlab, _outputGeneration, _outputGeneration);

            var writtenPayloads = await _segmentBuilder.WriteChangeSetAsync(message.ChangeSet);

            // Propagate changes downstream
            await _changesWrittenChannel.Writer.WriteAsync(
                new ChangesWritten(
                    Clock: message.Clock,
                    OutputSegment: _segmentBuilder.Segment,
                    Payloads: writtenPayloads,
                    Tombstones: message.ChangeSet.Tombstones));
        }

        // TODO: Terminate current output segment, if any

        // Propagate completion
        _changesWrittenChannel.Writer.Complete();
    }
}
