namespace Fugu.Utils;

public static class BootstrapSegmentOrderer
{
    public static IReadOnlyList<ISegmentMetadata> GetBootstrapOrder(IEnumerable<ISegmentMetadata> segments)
    {
        var byGeneration = segments
            .OrderBy(s => s.MinGeneration)
            .ThenBy(s => s.MaxGeneration);

        return [.. byGeneration];
    }
}
