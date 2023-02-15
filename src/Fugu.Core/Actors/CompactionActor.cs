using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class CompactionActor : Actor
{
    private readonly ChannelReader<SegmentStatsUpdatedMessage> _segmentStatsUpdatedChannelReader;
    private readonly ChannelReader<DummyMessage> _segmentEmptiedChannelReader;
    private readonly ChannelReader<DummyMessage> _snapshotsUpdatedChannelReader;
    private readonly ChannelWriter<UpdateIndexMessage> _updateIndexChannelWriter;
    private readonly ChannelWriter<DummyMessage> _segmentEvictedChannelWriter;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);

    public CompactionActor(
        ChannelReader<SegmentStatsUpdatedMessage> segmentStatsUpdatedChannelReader,
        ChannelReader<DummyMessage> segmentEmptiedChannelReader,
        ChannelReader<DummyMessage> snapshotsUpdatedChannelReader,
        ChannelWriter<UpdateIndexMessage> updateIndexChannelWriter,
        ChannelWriter<DummyMessage> segmentEvictedChannelWriter)
    {
        _segmentStatsUpdatedChannelReader = segmentStatsUpdatedChannelReader;
        _segmentEmptiedChannelReader = segmentEmptiedChannelReader;
        _snapshotsUpdatedChannelReader = snapshotsUpdatedChannelReader;
        _updateIndexChannelWriter = updateIndexChannelWriter;
        _segmentEvictedChannelWriter = segmentEvictedChannelWriter;
    }

    public override async Task RunAsync()
    {
        await Task.WhenAll(
            HandleSegmentStatsUpdatedMessagesAsync(),
            HandleSegmentEmptiedMessagesAsync(),
            HandleSnapshotsUpdatedMessagesAsync());

        // All input channels have completed, propagate completion
        _segmentEvictedChannelWriter.Complete();
    }

    private async Task HandleSegmentStatsUpdatedMessagesAsync()
    {
        while (await _segmentStatsUpdatedChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_segmentStatsUpdatedChannelReader.TryRead(out var message))
                {

                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleSegmentEmptiedMessagesAsync()
    {
        while (await _segmentEmptiedChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_segmentEmptiedChannelReader.TryRead(out var message))
                {

                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleSnapshotsUpdatedMessagesAsync()
    {
        while (await _snapshotsUpdatedChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_snapshotsUpdatedChannelReader.TryRead(out var message))
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
