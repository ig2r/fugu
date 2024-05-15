using Microsoft.Win32.SafeHandles;

namespace Fugu.IO;

public sealed class FileSlab : IWritableSlab, ISlab, IDisposable
{
    private readonly SafeFileHandle _handle;
    private readonly FileStream _stream;

    public FileSlab(string path)
    {
        // Create write-only output stream. This creates the file itself. When writing finishes, the
        // SegmentWriter will close the attached PipeWriter, which in turn will close this underlying stream.
        _stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.Read,
            Options = FileOptions.Asynchronous,
            BufferSize = 64 * 4096,
        });

        // Create a separate handle for reading. Note that we cannot use _stream.SafeFileHandle because the latter
        // will become invalid when the output stream is closed after completing a segment.
        _handle = File.OpenHandle(
            path,
            mode: FileMode.Open,
            access: FileAccess.Read,
            share: FileShare.ReadWrite,
            options: FileOptions.Asynchronous);
    }

    public Stream Output => _stream;

    public long Length => _stream.Length;

    public void Dispose()
    {
        // _stream is already disposed at this point because the attached writer has shut down and has closed its output stream.
        _handle.Dispose();
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long offset, CancellationToken cancellationToken = default)
    {
        return RandomAccess.ReadAsync(_handle, buffer, offset, cancellationToken);
    }
}
