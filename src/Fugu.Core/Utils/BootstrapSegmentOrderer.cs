namespace Fugu.Utils;

public static class BootstrapSegmentOrderer
{
    public static IReadOnlyList<ISegmentMetadata> GetBootstrapOrder(IEnumerable<ISegmentMetadata> segments)
    {
        var byGeneration = segments
            .OrderBy(s => s.MaxGeneration)
            .ThenByDescending(s => s.MinGeneration);

        var stack = new Stack<ISegmentMetadata>();

        foreach (var segment in byGeneration)
        {
            // If the current segment does not extend past the topmost segment already on the stack, discard it.
            if (stack.TryPeek(out var top) && segment.MaxGeneration <= top.MaxGeneration)
            {
                continue;
            }

            stack.Push(segment);
        }

        var result = stack.ToList();
        result.Reverse();

        return result;
    }
}
