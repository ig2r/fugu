using Fugu.Storage;
using System.Buffers;
using System.IO.Pipelines;

// Create a pre-populated PipeReader for testing
PipeReader CreateTestDataPipeReader()
{
    void BuildSegment(IBufferWriter<byte> bufferWriter)
    {
        var segmentWriter = new SegmentWriter(bufferWriter);
        segmentWriter.WriteSegmentHeader(1, 2);

        segmentWriter.WriteChangeSetHeader(1, 1);
        segmentWriter.WritePayloadHeader("foo"u8, valueLength: 20);
        segmentWriter.WriteTombstone("bar"u8);

        bufferWriter.Write(new byte[20]);
    }

    var bufferWriter = new ArrayBufferWriter<byte>();
    BuildSegment(bufferWriter);

    var pipe = new Pipe();

    var task = Task.Run(async () =>
    {
        for (int i = 0; i < bufferWriter.WrittenCount; i += 10)
        {
            var slice = bufferWriter.WrittenMemory.Slice(i, Math.Min(i + 10, bufferWriter.WrittenCount) - i);
            pipe.Writer.Write(slice.Span);
            await pipe.Writer.FlushAsync();

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        pipe.Writer.Complete();
    });

    return pipe.Reader;
}


var pipeReader = CreateTestDataPipeReader();
var parser = new SegmentParser();
await parser.ParseAsync(pipeReader);

Console.WriteLine("Done.");
