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

    public AllocationActor(IBackingStorage storage, Channel<ChangeSetAllocated> changeSetAllocatedChannel)
    {
        _storage = storage;
        _changeSetAllocatedChannel = changeSetAllocatedChannel;
    }

    public async Task RunAsync()
    {

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

            _outputSlab ??= await _storage.CreateSlabAsync();

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
