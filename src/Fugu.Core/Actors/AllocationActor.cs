using Fugu.Channels;
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
    private long _totalBytes = 0;

    public AllocationActor(
        IBackingStorage storage,
        Channel<ChangeSetAllocated> changeSetAllocatedChannel,
        long totalBytes)
    {
        _storage = storage;
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
        _totalBytes = totalBytes;
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

            // If we have reached the size limit for the current output slab, stop writing to it so we'll create a new one
            if (_outputSlab is not null && _outputSlabBytesWritten >= _outputSlabSizeLimit)
            {
                _totalBytes += _outputSlabBytesWritten;

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

                // Set the size limit for our new slab to the nth element of the geometric series. Derived from closed-form
                // formula for cumulative sum of (a, r) geometric series: S = a * (1 - r^n) / (1 - r)
                // ...solved for a * r^n.
                _outputSlabSizeLimit = (long)(a + _totalBytes * (r - 1));
             }

            _outputSlabBytesWritten += ChangeSetUtils.GetDataBytes(changeSet);

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
