namespace Fugu.Storage;

public interface IWritableSlab : ISlab
{
    // For writing: can expose a writable Stream instance. Consumers of this interface can call
    // PipeWriter.Create(stream) on that to obtain a PipeWriter, which implements IBufferWriter<byte>.
    // In total:
    // - This interface provides writable stream (= FileStream, internally)
    // - Writer actor constructs PipeWriter.Create(stream)
    // - Writer uses commit formatter that takes an IBufferWriter<byte>, which PipeWriter implements;
    //   so commit formatter could be a ref struct, etc. - totally sync
    // - Writer calls PipeWriter.FlushAsync when finished with a single commit
    // Some notes:
    // - PipeWriter will hand over accumulated data to stream.Write only when calling FlushAsync/WriteAsync.
    //   If PipeWriter needs to split data into multiple blocks, these blocks will be handed to the stream
    //   one-by-one.
    // - PipeWriter.FlushAsync internally calls FlushAsync on the wrapped stream. So no need to hold on to
    //   the stream to flush it explicitly.

    // For sequential reading: also provide a Stream instance, backed by FileStream for file I/O.
    // Consumers of this interface can instantiate a PipeReader via PipeReader.Create(stream).
    // BUT: PipeReader doesn't support seeking and/or skipping. Maybe better off to rely on standard
    // RandomAccess.ReadAsync method above.

    Stream Output { get; }
}
