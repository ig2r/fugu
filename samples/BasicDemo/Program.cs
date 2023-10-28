using Fugu.IO;
using System.Buffers;
using System.IO.Pipelines;

// Create a pre-populated PipeReader for testing
static async Task CreateTestDataAsync(PipeWriter pipeWriter)
{
    static void BuildSegment(IBufferWriter<byte> bufferWriter)
    {
        var segmentWriter = new SegmentWriter(bufferWriter);
        segmentWriter.WriteSegmentHeader(1, 2);

        segmentWriter.WriteChangeSetHeader(1, 1);
        segmentWriter.WritePayloadHeader("foo"u8, valueLength: 20);
        segmentWriter.WriteTombstone("bar"u8);

        bufferWriter.Write(new byte[20]);
    }

    BuildSegment(pipeWriter);
    await pipeWriter.FlushAsync();
    await pipeWriter.CompleteAsync();
}

// Create pipe & fill it with test data
var pipe = new Pipe();
_ = CreateTestDataAsync(pipe.Writer);

// Read back test data from pipe
var parser = new SegmentParser();
await parser.ParseAsync(pipe.Reader);

Console.WriteLine("Done.");
