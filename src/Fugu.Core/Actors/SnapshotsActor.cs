using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using System.Collections.Immutable;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SnapshotsActor : Actor
{
    private readonly ChannelReader<IndexUpdatedMessage> _indexUpdatedChannelReader;
    private readonly ChannelReader<AwaitClockMessage> _awaitClockChannelReader;
    private readonly ChannelReader<AcquireSnapshotMessage> _acquireSnapshotChannelReader;
    private readonly ChannelReader<DummyMessage> _releaseSnapshotChannelReader;
    private readonly ChannelWriter<DummyMessage> _snapshotsUpdatedChannelWriter;

    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
    private VectorClock _clock = new VectorClock();
    private Index _index = ImmutableDictionary<Key, IndexEntry>.Empty;

    public SnapshotsActor(
        ChannelReader<IndexUpdatedMessage> indexUpdatedChannelReader,
        ChannelReader<AwaitClockMessage> awaitClockChannelReader,
        ChannelReader<AcquireSnapshotMessage> acquireSnapshotChannelReader,
        ChannelReader<DummyMessage> releaseSnapshotChannelReader,
        ChannelWriter<DummyMessage> snapshotsUpdatedChannelWriter)
    {
        _indexUpdatedChannelReader = indexUpdatedChannelReader;
        _awaitClockChannelReader = awaitClockChannelReader;
        _acquireSnapshotChannelReader = acquireSnapshotChannelReader;
        _releaseSnapshotChannelReader = releaseSnapshotChannelReader;
        _snapshotsUpdatedChannelWriter = snapshotsUpdatedChannelWriter;
    }

    public override Task RunAsync()
    {
        return Task.WhenAll(
            HandleIndexUpdatedMessagesAsync(),
            HandleAwaitClockMessagesAsync(),
            HandleAcquireSnapshotMessagesAsync(),
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
                    _index = message.Index;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Input channel has completed, propagate completion
        _snapshotsUpdatedChannelWriter.Complete();
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
                    // TODO: Only signal this reply channel if our internal vector clock actually
                    // exceeds the requested threshold
                    await message.ReplyChannelWriter.WriteAsync(default);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    private async Task HandleAcquireSnapshotMessagesAsync()
    {
        while (await _acquireSnapshotChannelReader.WaitToReadAsync())
        {
            await _semaphore.WaitAsync();

            try
            {
                if (_acquireSnapshotChannelReader.TryRead(out var message))
                {
                    var snapshot = new Snapshot(_index, OnSnapshotDisposed);
                    await message.ReplyChannelWriter.WriteAsync(snapshot);
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

    private void OnSnapshotDisposed(Snapshot snapshot)
    {

    }
}
