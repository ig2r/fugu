using Fugu.Channels;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class SnapshotsActor
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;

    private VectorClock _clock;
    private readonly PriorityQueue<TaskCompletionSource, long> _pendingWaiters = new();

    public SnapshotsActor(Channel<IndexUpdated> indexUpdatedChannel)
    {
        _indexUpdatedChannel = indexUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _indexUpdatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _indexUpdatedChannel.Reader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                _clock = VectorClock.Max(_clock, message.Clock);

                // Release any waiters
                while (_pendingWaiters.TryPeek(out _, out var writeClock))
                {
                    if (_clock.Write >= writeClock)
                    {
                        var completionSource = _pendingWaiters.Dequeue();
                        completionSource.SetResult();
                    }
                    else
                    {
                        break;
                    }
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    public async ValueTask WaitForObservableEffectsAsync(VectorClock target)
    {
        TaskCompletionSource completionSource;
        await _semaphore.WaitAsync();

        try
        {
            if (_clock >= target)
            {
                return;
            }

            completionSource = new TaskCompletionSource();
            _pendingWaiters.Enqueue(completionSource, target.Write);
        }
        finally
        {
            _semaphore.Release();
        }

        await completionSource.Task;
    }
}
