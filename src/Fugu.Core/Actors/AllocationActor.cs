using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class AllocationActor : Actor
{
    private readonly ChannelReader<DummyMessage> _allocateWriteBatchChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEvictedChannelReader;
    private readonly ChannelWriter<DummyMessage> _writeWriteBatchChannelWriter;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);

    public AllocationActor(
        ChannelReader<DummyMessage> allocateWriteBatchChannelReader,
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
