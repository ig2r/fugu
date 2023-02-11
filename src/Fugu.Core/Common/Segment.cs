using Fugu.Core.IO;

namespace Fugu.Core.Common;

public class Segment
{
    public Segment(long minGeneration, long maxGeneration, ReadOnlyTable table)
    {
        MinGeneration = minGeneration;
        MaxGeneration = maxGeneration;
        Table = table;
    }

    public long MinGeneration { get; }
    public long MaxGeneration { get; }
    public ReadOnlyTable Table { get; }
}
