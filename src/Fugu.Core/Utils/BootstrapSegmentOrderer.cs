namespace Fugu.Utils;

public static class BootstrapSegmentOrderer
{
    /// <summary>
    /// Given a collection of segments available in storage, determines which of these segments to load (and in
    /// which order) to restore the contents of the store.
    /// </summary>
    /// <param name="segments">Metadata for segments available in storage.</param>
    /// <returns>An ordered list of segments to load.</returns>
    /// <exception cref="InvalidOperationException">The algorithm encountered an invalid state.</exception>
    public static IReadOnlyList<ISegmentMetadata> GetBootstrapOrder(IEnumerable<ISegmentMetadata> segments)
    {
        long maxGenerationAccepted = 0;

        // Order segments so that for each max generation, shorter segments (= higher min generation) are evaluated
        // before looking at longer-range segments that extend to the same max generation. As a consequence, the
        // algorithm will forgo longer segments arising from compactions if all of the source segments are still
        // available. Only when it detects gaps in the sequence of smaller segments will it use a longer-range
        // segment instead.
        var byGeneration = new Queue<ISegmentMetadata>(
            segments
                .OrderBy(s => s.MaxGeneration)
                .ThenByDescending(s => s.MinGeneration));

        var stack = new Stack<ISegmentMetadata>();

        while (byGeneration.TryDequeue(out var segment))
        {
            // If the current segment does not extend past the topmost segment already on the stack, discard it.
            if (segment.MaxGeneration <= maxGenerationAccepted)
            {
                continue;
            }

            // If there is a gap between the max accepted generation and the lower generation bound of the current segment,
            // we expect that this will be covered by an upcoming longer-range segment.
            if (segment.MinGeneration > maxGenerationAccepted + 1)
            {
                while (segment.MinGeneration > maxGenerationAccepted + 1)
                {
                    if (!byGeneration.TryDequeue(out segment))
                    {
                        throw new InvalidOperationException("Gap found, unable to close by a covering segment.");
                    }
                }
            }

            // This segment might overlap previous segments already on the stack, so we need to discard items from
            // the stack to resolve overlaps.
            while (stack.TryPeek(out var topItem) && topItem.MaxGeneration >= segment.MinGeneration)
            {
                stack.Pop();
            }

            stack.Push(segment);
            maxGenerationAccepted = segment.MaxGeneration;
        }

        return stack.Reverse().ToArray();
    }
}
