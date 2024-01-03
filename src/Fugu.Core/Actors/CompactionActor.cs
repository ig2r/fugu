using Fugu.Channels;
using System.Threading.Channels;

namespace Fugu.Actors;

public sealed class CompactionActor
{
    private readonly Channel<SegmentStatsUpdated> _segmentStatsUpdatedChannel;

    public CompactionActor(Channel<SegmentStatsUpdated> segmentStatsUpdatedChannel)
    {
        _segmentStatsUpdatedChannel = segmentStatsUpdatedChannel;
    }

    public async Task RunAsync()
    {
        while (await _segmentStatsUpdatedChannel.Reader.WaitToReadAsync())
        {
            var message = await _segmentStatsUpdatedChannel.Reader.ReadAsync();

        }
    }
}
