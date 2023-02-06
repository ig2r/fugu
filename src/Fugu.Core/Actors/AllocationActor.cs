using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using Fugu.Core.IO;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class AllocationActor : Actor
{
    private readonly ChannelReader<AllocateWriteBatchMessage> _allocateWriteBatchChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEvictedChannelReader;
    private readonly ChannelWriter<WriteWriteBatchMessage> _writeWriteBatchChannelWriter;
    private readonly TableSet _tableSet;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
    private VectorClock _clock = new VectorClock();
    private Table? _outputTable = null;

    public AllocationActor(
        ChannelReader<AllocateWriteBatchMessage> allocateWriteBatchChannelReader,
        ChannelReader<DummyMessage> segmentEvictedChannelReader,
        ChannelWriter<WriteWriteBatchMessage> writeWriteBatchChannelWriter,
        TableSet tableSet)
    {
        _allocateWriteBatchChannelReader = allocateWriteBatchChannelReader;
        _segmentEvictedChannelReader = segmentEvictedChannelReader;
        _writeWriteBatchChannelWriter = writeWriteBatchChannelWriter;
        _tableSet = tableSet;
    }

    public override Task RunAsync()
    {
        return Task.WhenAll(
            HandleAllocateWriteBatchMessagesAsync(),
            HandleSegmentEvictedMessagesAsync());
    }

    private async Task HandleAllocateWriteBatchMessagesAsync()
    {
        while (await _allocateWriteBatchChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_allocateWriteBatchChannelReader.TryRead(out var message))
                {
                    // Step clock and tell sender. If they require a fully-consistent view of the data, i.e.,
                    // "read your own write" consistency, they can wait until this timestamp becomes visible
                    // in external snapshots.
                    _clock = _clock with { Write = _clock.Write + 1 };
                    await message.ReplyChannelWriter.WriteAsync(_clock);

                    // Make sure there's an output table with sufficient remaining space for this write
                    if (_outputTable is null)
                    {
                        _outputTable = await _tableSet.CreateTableAsync(1024);
                    }

                    await _writeWriteBatchChannelWriter.WriteAsync(
                        new WriteWriteBatchMessage
                        {
                            Batch = message.Batch,
                            Clock = _clock,
                            OutputTable = _outputTable,
                        });
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Input channel has completed, propagate completion
        _writeWriteBatchChannelWriter.Complete();
    }

    private async Task HandleSegmentEvictedMessagesAsync()
    {
        while (await _segmentEvictedChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_segmentEvictedChannelReader.TryRead(out var message))
                {
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}
