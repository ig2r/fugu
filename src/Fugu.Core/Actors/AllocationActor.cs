using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class AllocationActor : Actor
{
    private readonly ChannelReader<AllocateWriteBatchMessage> _allocateWriteBatchChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEvictedChannelReader;
    private readonly ChannelWriter<DummyMessage> _writeWriteBatchChannelWriter;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
    private VectorClock _clock = new VectorClock();

    public AllocationActor(
        ChannelReader<AllocateWriteBatchMessage> allocateWriteBatchChannelReader,
        ChannelReader<DummyMessage> segmentEvictedChannelReader,
        ChannelWriter<DummyMessage> writeWriteBatchChannelWriter)
    {
        _allocateWriteBatchChannelReader = allocateWriteBatchChannelReader;
        _segmentEvictedChannelReader = segmentEvictedChannelReader;
        _writeWriteBatchChannelWriter = writeWriteBatchChannelWriter;
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
                    _clock = _clock with { Write = _clock.Write + 1 };
                    await message.ReplyChannelWriter.WriteAsync(_clock);
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
