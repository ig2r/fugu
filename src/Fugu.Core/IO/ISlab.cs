namespace Fugu.IO;

public interface ISlab
{
    long Length { get; }

    // To be implemented by RandomAccess.ReadAsync for file I/O
    ValueTask<int> ReadAsync(Memory<byte> buffer, long offset, CancellationToken cancellationToken = default);
}
