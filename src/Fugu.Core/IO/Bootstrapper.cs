using Fugu.Channels;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.IO;

public static class Bootstrapper
{
    public static async Task<BootstrapResult> InitializeStoreAsync(
        IBackingStorage storage,
        Channel<ChangesWritten> changesWrittenChannel)
    {
        var slabs = await storage.GetAllSlabsAsync();
        var segmentReaders = new Dictionary<ISegmentMetadata, SegmentReader>(capacity: slabs.Count);

        foreach (var slab in slabs)
        {
            var reader = await SegmentReader.CreateAsync(slab);
            segmentReaders[reader.Segment] = reader;
        }

        // Order segments, decide on which ones to load & which ones to skip.
        // TODO: Discard skipped segments.
        var segmentsToLoad = BootstrapSegmentOrderer.GetBootstrapOrder(segmentReaders.Keys);

        long maxGeneration = segmentsToLoad.Count > 0
            ? segmentsToLoad.Max(s => s.MaxGeneration)
            : 0;

        long totalBytes = 0;

        // For all change sets across all segments in order, feed these change sets to index actor.
        foreach (var segmentMetadata in segmentsToLoad)
        {
            var reader = segmentReaders[segmentMetadata];
            await foreach (var changes in reader.ReadChangeSetsAsync())
            {
                totalBytes += ChangeSetUtils.GetDataBytes(changes);

                var changesWritten = new ChangesWritten(
                    new VectorClock(0, 0),
                    reader.Segment,
                    changes.Payloads,
                    changes.Tombstones);

                await changesWrittenChannel.Writer.WriteAsync(changesWritten);
            }
        }

        return new BootstrapResult(maxGeneration, totalBytes);
    }
}
