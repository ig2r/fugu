using Fugu.Channels;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class SnapshotsActor
{
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;

    public SnapshotsActor(Channel<IndexUpdated> indexUpdatedChannel)
    {
        _indexUpdatedChannel = indexUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _indexUpdatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _indexUpdatedChannel.Reader.ReadAsync();
        }
    }
}
