using Fugu.Channels;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class SnapshotsActor : ISnapshotOwner
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly ChannelReader<IndexUpdated> _indexUpdatedChannelReader;
    private readonly ChannelWriter<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannelWriter;

    private VectorClock _clock;
    private IReadOnlyDictionary<byte[], IndexEntry> _index = new Dictionary<byte[], IndexEntry>();

    private readonly SortedDictionary<VectorClock, int> _activeSnapshotsByClock = new(
        Comparer<VectorClock>.Create((x, y) =>
        {
            var componentComparer = Comparer<long>.Default;
            var writeComparison = componentComparer.Compare(x.Write, y.Write);

            return writeComparison != 0
                ? writeComparison
                : componentComparer.Compare(x.Compaction, y.Compaction);
        }));

    private readonly PriorityQueue<TaskCompletionSource, VectorClock> _pendingWaiters = new(
        Comparer<VectorClock>.Create((x, y) =>
        {
            var componentComparer = Comparer<long>.Default;
            var writeComparison = componentComparer.Compare(x.Write, y.Write);

            return writeComparison != 0
                ? writeComparison
                : componentComparer.Compare(x.Compaction, y.Compaction);
        }));

    public SnapshotsActor(
        Channel<IndexUpdated> indexUpdatedChannel,
        Channel<OldestObservableSnapshotChanged> oldestObservableSnapshotChangedChannel)
    {
        _indexUpdatedChannelReader = indexUpdatedChannel.Reader;
        _oldestObservableSnapshotChangedChannelWriter = oldestObservableSnapshotChangedChannel.Writer;
    }

    public async Task RunAsync()
    {
        while (await _indexUpdatedChannelReader.WaitToReadAsync())
        {
            var message = await _indexUpdatedChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                var compactionClockStepped = message.Clock.Compaction > _clock.Compaction;

                _clock = VectorClock.Max(_clock, message.Clock);
                _index = message.Index;

                // Release any waiters that have been stalled until this message's clock value
                // becomes observable, indicating that the changes have been written and the
                // index has been updated.
                while (_pendingWaiters.TryPeek(out _, out var topItemClock))
                {
                    if (_clock >= topItemClock)
                    {
                        var completionSource = _pendingWaiters.Dequeue();
                        completionSource.SetResult();
                    }
                    else
                    {
                        break;
                    }
                }

                // If there are no active snapshots and compaction clock component has stepped,
                // tell compaction actor that nothing before the current message's clock can be
                // observed anymore.
                if (compactionClockStepped && _activeSnapshotsByClock.Count == 0)
                {
                    await _oldestObservableSnapshotChangedChannelWriter.WriteAsync(
                        new OldestObservableSnapshotChanged(_clock));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // TODO: Maybe wait until all open snapshots have been disposed?
        // Or throw if there are any open snapshots around?

        _oldestObservableSnapshotChangedChannelWriter.Complete();
    }

    public async ValueTask WaitForObservableEffectsAsync(VectorClock threshold)
    {
        TaskCompletionSource completionSource;
        await _semaphore.WaitAsync();

        try
        {
            if (_clock >= threshold)
            {
                return;
            }

            completionSource = new TaskCompletionSource();
            _pendingWaiters.Enqueue(completionSource, threshold);
        }
        finally
        {
            _semaphore.Release();
        }

        await completionSource.Task;
    }

    public async ValueTask<Snapshot> GetSnapshotAsync()
    {
        await _semaphore.WaitAsync();

        try
        {
            var snapshot = new Snapshot(this, _clock, _index);

            // TODO: Remember this snapshot because we can't remove any segments that can see it
            _activeSnapshotsByClock[_clock] = _activeSnapshotsByClock.TryGetValue(_clock, out var count) ? count + 1 : 1;

            return snapshot;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async void OnSnapshotDisposed(Snapshot snapshot)
    {
        await _semaphore.WaitAsync();

        try
        {
            // TODO: Remove this snapshot and notify compaction actor we're good to release
            // any segments that were retained due to being visible in this snapshot
            var remainingCount = _activeSnapshotsByClock[snapshot.Clock] - 1;

            if (remainingCount == 0)
            {
                _activeSnapshotsByClock.Remove(snapshot.Clock);

                // TODO: If dictionary is now empty OR first element is bigger than snapshot's clock value,
                // notify compaction actor
                var oldestClock = _activeSnapshotsByClock.Keys.DefaultIfEmpty(_clock).First();
                if (oldestClock >= snapshot.Clock)
                {
                    await _oldestObservableSnapshotChangedChannelWriter.WriteAsync(
                        new OldestObservableSnapshotChanged(oldestClock)
                    );
                }
            }
            else
            {
                _activeSnapshotsByClock[snapshot.Clock] = remainingCount;
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
