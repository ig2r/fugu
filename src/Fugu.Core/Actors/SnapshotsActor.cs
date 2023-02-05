using Fugu.Core.Actors.Messages;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class SnapshotsActor : Actor
{
    private readonly ChannelReader<DummyMessage> _indexUpdatedChannelReader;
    private readonly ChannelReader<DummyMessage> _awaitClockChannelReader;
    private readonly ChannelReader<DummyMessage> _getSnapshotChannelReader;
    private readonly ChannelReader<DummyMessage> _releaseSnapshotChannelReader;
    private readonly ChannelWriter<DummyMessage> _snapshotsUpdatedChannelWriter;

    public SnapshotsActor(
        ChannelReader<DummyMessage> indexUpdatedChannelReader,
        ChannelReader<DummyMessage> awaitClockChannelReader,
        ChannelReader<DummyMessage> getSnapshotChannelReader,
        ChannelReader<DummyMessage> releaseSnapshotChannelReader,
        ChannelWriter<DummyMessage> snapshotsUpdatedChannelWriter)
    {
        _indexUpdatedChannelReader = indexUpdatedChannelReader;
        _awaitClockChannelReader = awaitClockChannelReader;
        _getSnapshotChannelReader = getSnapshotChannelReader;
        _releaseSnapshotChannelReader = releaseSnapshotChannelReader;
        _snapshotsUpdatedChannelWriter = snapshotsUpdatedChannelWriter;
    }

    public override Task RunAsync()
    {
        throw new NotImplementedException();
    }
}
