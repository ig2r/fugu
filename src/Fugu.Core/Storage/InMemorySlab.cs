using System.Buffers;

namespace Fugu.Storage;

public class InMemorySlab : IWritableStorageSlab
{
    private readonly ArrayBufferWriter<byte> _arrayBufferWriter;
    private readonly ReaderWriterLockSlim _readerWriterLock;

    internal InMemorySlab()
    {
        _arrayBufferWriter = new ArrayBufferWriter<byte>();
        _readerWriterLock = new ReaderWriterLockSlim();

        Output = new BufferWriterStream(_arrayBufferWriter, _readerWriterLock);
    }

    public long Length
    {
        get
        {
            _readerWriterLock.EnterReadLock();

            try
            {
                return _arrayBufferWriter.WrittenCount;
            }
            finally
            {
                _readerWriterLock.ExitReadLock();
            }
        }
    }

    public Stream Output { get; }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long offset, CancellationToken cancellationToken = default)
    {
        _readerWriterLock.EnterReadLock();

        try
        {
            _arrayBufferWriter.WrittenSpan.Slice((int)offset, buffer.Length).CopyTo(buffer.Span);
            return ValueTask.FromResult(buffer.Length);
        }
        finally
        {
            _readerWriterLock.ExitReadLock();
        }
    }

    private class BufferWriterStream : Stream
    {
        private readonly IBufferWriter<byte> _bufferWriter;
        private readonly ReaderWriterLockSlim _readerWriterLock;

        public BufferWriterStream(IBufferWriter<byte> bufferWriter, ReaderWriterLockSlim readerWriterLock)
        {
            _bufferWriter = bufferWriter;
            _readerWriterLock = readerWriterLock;
        }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _readerWriterLock.EnterWriteLock();

            try
            {
                _bufferWriter.Write(buffer.AsSpan().Slice(offset, count));
            }
            finally
            {
                _readerWriterLock.ExitWriteLock();
            }
        }
    }
}
