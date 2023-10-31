using Fugu.Channels;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class IndexActor
{
    private readonly Channel<ChangesWritten> _changesWrittenChannel;
    private readonly Channel<IndexUpdated> _indexUpdatedChannel;

    public IndexActor(Channel<ChangesWritten> changesWrittenChannel, Channel<IndexUpdated> indexUpdatedChannel)
    {
        _changesWrittenChannel = changesWrittenChannel;
        _indexUpdatedChannel = indexUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _changesWrittenChannel.Reader.WaitToReadAsync())
        {
            var message = await _changesWrittenChannel.Reader.ReadAsync();

            await _indexUpdatedChannel.Writer.WriteAsync(
                new IndexUpdated(
                    Clock: message.Clock));
        }
    }
}
