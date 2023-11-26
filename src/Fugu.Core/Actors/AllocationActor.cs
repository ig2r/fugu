using Fugu.Channels;
using Fugu.IO;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class AllocationActor
{
    private const float GrowthFactor = 1.2f;

    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly IBackingStorage _storage;
    private readonly Channel<ChangeSetAllocated> _changeSetAllocatedChannel;
    
    private VectorClock _clock = new(Write: 0, Compaction: 0);

    private IWritableSlab? _outputSlab = null;

    // Used in partitioning decisions, i.e., when to close out the current output slab and switch to a new one.
    // TODO: Number of change sets is a pretty crude metric, but useful during development. Consider metrics such as
    // payload size (in bytes) instead.
    private long _changeSetsInCurrentOutputSlab = 0;
    private long _totalChangeSetsInStore = 0;

    public AllocationActor(IBackingStorage storage, Channel<ChangeSetAllocated> changeSetAllocatedChannel)
    {
        _storage = storage;
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
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

            _totalChangeSetsInStore++;

            // Depending on the total volume of data already in the store, new segments are allowed to have progressively
            // bigger capacity. As a result, the total number of segments that make up a store will be logarithmic in the
            // total number of change sets written, thus ensuring that as the store grows, new segments will be added at a
            // slower pace and that the number of open segments (= file handles) will stay within reasonable limits for
            // typical data volumes.
            var maxChangeSetsInCurrentOutputSlab = 1 + Math.Pow(_totalChangeSetsInStore - _changeSetsInCurrentOutputSlab, GrowthFactor);
            if (_changeSetsInCurrentOutputSlab > maxChangeSetsInCurrentOutputSlab)
            {
                _outputSlab = null;
                _changeSetsInCurrentOutputSlab = 0;
            }

            _outputSlab ??= await _storage.CreateSlabAsync();
            _changeSetsInCurrentOutputSlab++;

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
