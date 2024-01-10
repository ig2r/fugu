﻿using Fugu.Channels;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class SnapshotsActor : ISnapshotOwner
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;
    private readonly Channel<OldestObservableSnapshotChanged> _oldestObservableSnapshotChangedChannel;

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
        _indexUpdatedChannel = indexUpdatedChannel;
        _oldestObservableSnapshotChangedChannel = oldestObservableSnapshotChangedChannel;
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
                _index = message.Index;

                // Release any waiters
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
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // TODO: Maybe wait until all open snapshots have been disposed?
        // Or throw if there are any open snapshots around?

        _oldestObservableSnapshotChangedChannel.Writer.Complete();
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
                    await _oldestObservableSnapshotChangedChannel.Writer.WriteAsync(
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
