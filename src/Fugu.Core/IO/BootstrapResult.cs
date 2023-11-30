namespace Fugu.IO;

public sealed class BootstrapResult
{
    public BootstrapResult(long maxGeneration, long totalBytes)
    {
        MaxGeneration = maxGeneration;
        TotalBytes = totalBytes;
    }

    public long MaxGeneration { get; }
    public long TotalBytes { get; }
}
