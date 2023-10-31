using Fugu.IO;

namespace Fugu;

public sealed class Segment
{
    public Segment(long minGeneration, long maxGeneration, ISlab slab)
    {
        MinGeneration = minGeneration;
        MaxGeneration = maxGeneration;
        Slab = slab;
    }

    public long MinGeneration { get; }

    public long MaxGeneration { get; }
    
    public ISlab Slab { get; }
}
