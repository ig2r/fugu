namespace Fugu.Utils;

/// <summary>
/// Computes size thresholds to ensure that the number of segments in a store remains within reasonable bounds.
/// </summary>
public sealed class BalancingStrategy
{
    private readonly long _coefficient;
    private readonly double _growthFactor;

    /// <summary>
    /// Initializes a new instance of the BalancingStrategy class.
    /// </summary>
    /// <param name="coefficient">Coefficient of the geometric series, also the size of the first segment in bytes.</param>
    /// <param name="growthFactor">Indicates by how much each added segment should be bigger than the previous one.</param>
    /// <exception cref="ArgumentOutOfRangeException">A parameter was invalid.</exception>
    public BalancingStrategy(long coefficient, double growthFactor)
    {
        if (coefficient <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(coefficient), "Coefficient must be greater than zero.");
        }

        if (growthFactor <= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(growthFactor), "Growth factor must be greater than one.");
        }

        _coefficient = coefficient;
        _growthFactor = growthFactor;
    }

    /// <summary>
    /// Assuming a store with a given number of segments, calculates the total capacity of these segments if they were
    /// sized according to elements of a geometric series.
    /// </summary>
    /// <param name="segmentCount">The number of segments.</param>
    /// <returns>The idealized capacity of a store with <c>segmentCount</c> segments.</returns>
    public long GetIdealizedCapacity(int segmentCount)
    {
        // Given the current number of non-output segments in the store, calculate idealized capacity of the store as the
        // cumulative sum of an n-element (a, r) geometric series:
        var capacity = _coefficient * (1 - Math.Pow(_growthFactor, segmentCount)) / (1 - _growthFactor);
        return (long)capacity;
    }

    /// <summary>
    /// Assuming a store with a total number of <c>totalBytes</c> bytes, calculates the size limit for the next segment
    /// when adding a new segment to the store.
    /// </summary>
    /// <param name="totalBytes">Total volume of data held by the store.</param>
    /// <returns>Size limit for a new segment.</returns>
    public long GetOutputSizeLimit(long totalBytes)
    {
        // Set the size limit for our new slab to the nth element of the geometric series. Derived from closed-form
        // formula for cumulative sum of (a, r) geometric series: S = a * (1 - r^n) / (1 - r) solved for a * r^n.
        return (long)(_coefficient + totalBytes * (_growthFactor - 1));
    }
}
