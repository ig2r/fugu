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
            var snapshot = new Snapshot(this, _index);
            return snapshot;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void OnSnapshotDisposed(Snapshot snapshot)
    {
        // TODO: Implement this
    }
}
