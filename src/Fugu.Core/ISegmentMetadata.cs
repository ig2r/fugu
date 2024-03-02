namespace Fugu;

public interface ISegmentMetadata
{
    long MaxGeneration { get; }
    long MinGeneration { get; }
}