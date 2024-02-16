using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class AllocationActor
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly IBackingStorage _storage;
    private readonly BalancingStrategy _balancingStrategy;
    private readonly ChannelReader<SegmentsCompacted> _segmentsCompactedChannelReader;
    private readonly ChannelWriter<ChangeSetAllocated> _changeSetAllocatedChannelWriter;
    
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
        BalancingStrategy balancingStrategy,
        ChannelReader<SegmentsCompacted> segmentsCompactedChannelReader,
        ChannelWriter<ChangeSetAllocated> changeSetAllocatedChannelWriter,
        long totalBytes)
    {
        _storage = storage;
        _balancingStrategy = balancingStrategy;
        _segmentsCompactedChannelReader = segmentsCompactedChannelReader;
        _changeSetAllocatedChannelWriter = changeSetAllocatedChannelWriter;
        _totalBytes = totalBytes;
    }

    public async Task CompleteAsync()
    {
        await _semaphore.WaitAsync();

        try
        {
            _changeSetAllocatedChannelWriter.Complete();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task RunAsync()
    {
        while (await _segmentsCompactedChannelReader.WaitToReadAsync())
        {
            var message = await _segmentsCompactedChannelReader.ReadAsync();
            await _semaphore.WaitAsync();

            try
            {
                _totalBytes += message.CapacityChange;

                // Readjust the output size limit for the current output right away, instead of letting
                // it complete and then sizing the following segment only. This can mean that the current
                // output segment is "cut short", because it is now suddenly over limit.
                _outputSlabSizeLimit = _balancingStrategy.GetOutputSizeLimit(_totalBytes);
            }
            finally
            {
                _semaphore.Release();
            }
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
                _outputSlabSizeLimit = _balancingStrategy.GetOutputSizeLimit(_totalBytes);
            }

            _outputSlabBytesWritten += ChangeSetUtils.GetDataBytes(changeSet);

            await _changeSetAllocatedChannelWriter.WriteAsync(
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
