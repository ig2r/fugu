using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SnapshotsActor : Actor
{
    private readonly ChannelReader<DummyMessage> _indexUpdatedChannelReader;
    private readonly ChannelReader<DummyMessage> _awaitClockChannelReader;
    private readonly ChannelReader<DummyMessage> _getSnapshotChannelReader;
    private readonly ChannelReader<DummyMessage> _releaseSnapshotChannelReader;
    private readonly ChannelWriter<DummyMessage> _snapshotsUpdatedChannelWriter;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);

    public SnapshotsActor(
        ChannelReader<DummyMessage> indexUpdatedChannelReader,
        ChannelReader<DummyMessage> awaitClockChannelReader,
        ChannelReader<DummyMessage> getSnapshotChannelReader,
        ChannelReader<DummyMessage> releaseSnapshotChannelReader,
        ChannelWriter<DummyMessage> snapshotsUpdatedChannelWriter)
    {
        _indexUpdatedChannelReader = indexUpdatedChannelReader;
        _awaitClockChannelReader = awaitClockChannelReader;
        _getSnapshotChannelReader = getSnapshotChannelReader;
        _releaseSnapshotChannelReader = releaseSnapshotChannelReader;
        _snapshotsUpdatedChannelWriter = snapshotsUpdatedChannelWriter;
    }

    public override Task RunAsync()
    {
        return Task.WhenAll(
            HandleIndexUpdatedMessagesAsync(),
            HandleAwaitClockMessagesAsync(),
            HandleGetSnapshotMessagesAsync(),
            HandleReleaseSnapshotMessagesAsync());
    }

    private async Task HandleIndexUpdatedMessagesAsync()
    {
        while (await _indexUpdatedChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_indexUpdatedChannelReader.TryRead(out var message))
                {

                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleAwaitClockMessagesAsync()
    {
        while (await _awaitClockChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_awaitClockChannelReader.TryRead(out var message))
                {

                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleGetSnapshotMessagesAsync()
    {
        while (await _getSnapshotChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_getSnapshotChannelReader.TryRead(out var message))
                {

                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleReleaseSnapshotMessagesAsync()
    {
        while (await _releaseSnapshotChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_releaseSnapshotChannelReader.TryRead(out var message))
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
