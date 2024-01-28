using Fugu.Channels;
using Fugu.Utils;
using System.Threading.Channels;

namespace Fugu.IO;

public static class Bootstrapper
{
    public static async Task<BootstrapResult> LoadFromStorageAsync(
        IBackingStorage storage, Channel<ChangesWritten> changesWrittenChannel)
    {
        var slabs = await storage.GetAllSlabsAsync();
        var segmentParsers = new List<SegmentParser>(capacity: slabs.Count);

        foreach (var slab in slabs)
        {
			segmentParsers.Add(await SegmentParser.CreateAsync(slab));
        }

        // Order segments, decide on which ones to load & which ones to skip
        // TODO: The current implementation assumes that generations will never overlap, hence comparing MinGeneration
        // is sufficient to establish proper ordering. This assumption will NO LONGER BE VALID once we implement compaction.
        segmentParsers.Sort((x, y) => Comparer<long>.Default.Compare(x.Segment.MinGeneration, y.Segment.MinGeneration));

        long maxGeneration = segmentParsers.Count > 0 ? segmentParsers.Max(s => s.Segment.MaxGeneration) : 0;
        long totalBytes = 0;

        // For all change sets across all segments in order, feed these change sets to index actor
        foreach (var parser in segmentParsers)
        {
            await foreach (var changes in parser.ReadChangeSetsAsync())
            {
                totalBytes += changes.Payloads.Sum(p => p.Key.Length + p.Value.Length) + changes.Tombstones.Sum(t => t.Length);

                var changesWritten = new ChangesWritten(
                    new VectorClock(0, 0),
                    parser.Segment,
                    changes.Payloads,
                    changes.Tombstones.ToHashSet());

                await changesWrittenChannel.Writer.WriteAsync(changesWritten);
            }
        }

        return new BootstrapResult(maxGeneration, totalBytes);
    }
}
