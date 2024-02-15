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
        var segmentReaders = new List<SegmentReader>(capacity: slabs.Count);

        foreach (var slab in slabs)
        {
            var reader = await SegmentReader.CreateAsync(slab);
            segmentReaders.Add(reader);
        }

        // Order segments, decide on which ones to load & which ones to skip.
        // TODO: The current implementation assumes that generations will never overlap, hence comparing MinGeneration
        // is sufficient to establish proper ordering. This assumption will NO LONGER BE VALID once we implement compaction.
        segmentReaders.Sort((x, y) => Comparer<long>.Default.Compare(x.Segment.MinGeneration, y.Segment.MinGeneration));

        long maxGeneration = segmentReaders.Count > 0
            ? segmentReaders.Max(s => s.Segment.MaxGeneration)
            : 0;

        long totalBytes = 0;

        // For all change sets across all segments in order, feed these change sets to index actor.
        foreach (var reader in segmentReaders)
        {
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
