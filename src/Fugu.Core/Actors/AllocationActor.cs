﻿using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class AllocationActor
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly IBackingStorage _storage;
    private readonly Channel<ChangeSetAllocated> _changeSetAllocatedChannel;
    
    private VectorClock _clock = new(Write: 0, Compaction: 0);

    private IWritableSlab? _outputSlab = null;

    // Amount of data written to current output slab vs. size limit after which we'll roll over to a new output slab
    private long _outputSlabBytesWritten = 0;
    private long _outputSlabSizeLimit = 0;

    // Measures the total amount of data written to the store, across all segments. Note that this is updated only
    // when switching to a new output slab, not on every submitted change set.
    private long _totalBytesWritten = 0;

    public AllocationActor(
        IBackingStorage storage,
        Channel<ChangeSetAllocated> changeSetAllocatedChannel,
        long totalBytes)
    {
        _storage = storage;
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
        _totalBytesWritten = totalBytes;
    }

    public Task RunAsync()
    {
        return Task.CompletedTask;
    }

    public async Task CompleteAsync()
    {
        await _semaphore.WaitAsync();

        try
        {
            _changeSetAllocatedChannel.Writer.Complete();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async ValueTask<VectorClock> EnqueueChangeSetAsync(ChangeSet changeSet)
    {
        await _semaphore.WaitAsync();

        try
        {
            _clock = _clock with
            {
                Write = _clock.Write + 1,
            };

            // Number of key/value bytes in current change set
            var changeSetBytes = changeSet.Payloads.Sum(p => p.Key.Length + p.Value.Length) + changeSet.Tombstones.Sum(t => t.Length);

            // If we have reached the size limit for the current output slab, stop writing to it so we'll create a new one
            if (_outputSlab is not null && _outputSlabBytesWritten >= _outputSlabSizeLimit)
            {
                _totalBytesWritten += _outputSlabBytesWritten;

                _outputSlab = null;
                _outputSlabBytesWritten = 0;
            }

            if (_outputSlab is null)
            {
                _outputSlab = await _storage.CreateSlabAsync();

                // Determine the size limit for the new output slab based on a geometric series, which is characterized by
                // two parameters a and r:
                const double a = 100;       // Coefficient, also the size of slab #0
                const double r = 1.5;       // Common ratio, indicates by how much each added slab should be bigger than the last

                // Derive the idealized sequence index n at which the geometric series with parameters (a, r) would have the
                // cumulative sum of _totalBytesWritten. Note that n will most likely not be a whole number.
                double n = Math.Log(1 - ((1 - r) * _totalBytesWritten / a), r);

                // Set the size limit for our new slab to the nth element of the geometric series:
                _outputSlabSizeLimit = (long)(a * Math.Pow(r, n));
            }

            _outputSlabBytesWritten += changeSetBytes;

            await _changeSetAllocatedChannel.Writer.WriteAsync(
                new ChangeSetAllocated(
                    Clock: _clock,
                    ChangeSet: changeSet,
                    OutputSlab: _outputSlab));

            return _clock;
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
