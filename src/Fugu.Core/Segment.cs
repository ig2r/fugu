using Fugu.IO;

namespace Fugu;

public sealed class Segment : ISegmentMetadata
{
    public Segment(long minGeneration, long maxGeneration, ISlab slab)
    {
        if (minGeneration > maxGeneration)
        {
            throw new ArgumentException("Min generation cannot exceed max generation.");
        }

        MinGeneration = minGeneration;
        MaxGeneration = maxGeneration;
        Slab = slab;
    }

    public long MinGeneration { get; }

    public long MaxGeneration { get; }

    public ISlab Slab { get; }
}
