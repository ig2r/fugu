using System.Buffers;

namespace Fugu.Storage;

public class InMemorySlab : IWritableStorageSlab
{
    private readonly ArrayBufferWriter<byte> _arrayBufferWriter;
    private readonly ReaderWriterLockSlim _bufferWriterLock;

    internal InMemorySlab()
    {
        _arrayBufferWriter = new ArrayBufferWriter<byte>();
        _bufferWriterLock = new ReaderWriterLockSlim();

        Output = new BufferWriterStream(_arrayBufferWriter, _bufferWriterLock);
    }

    public long Length
    {
        get
        {
            _bufferWriterLock.EnterReadLock();

            try
            {
                return _arrayBufferWriter.WrittenCount;
            }
            finally
            {
                _bufferWriterLock.ExitReadLock();
            }
        }
    }

    public Stream Output { get; }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long offset, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    private class BufferWriterStream : Stream
    {
        private readonly IBufferWriter<byte> _bufferWriter;
        private readonly ReaderWriterLockSlim _bufferWriterLock;

        public BufferWriterStream(IBufferWriter<byte> bufferWriter, ReaderWriterLockSlim bufferWriterLock)
        {
            _bufferWriter = bufferWriter;
            _bufferWriterLock = bufferWriterLock;
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
            _bufferWriterLock.EnterWriteLock();

            try
            {
                _bufferWriter.Write(buffer.AsSpan().Slice(offset, count));
            }
            finally
            {
                _bufferWriterLock.ExitWriteLock();
            }
        }
    }
}
