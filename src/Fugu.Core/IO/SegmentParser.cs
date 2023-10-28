using System.IO.Pipelines;

namespace Fugu.IO;

public class SegmentParser
{
    public async ValueTask ParseAsync(PipeReader pipeReader, CancellationToken cancellationToken = default)
    {
        ReadResult readResult;
        var context = new ParseContext();

        do
        {
            readResult = await pipeReader.ReadAsync(cancellationToken);
            var consumed = ParseReadResult(in readResult, context);
            pipeReader.AdvanceTo(consumed, examined: readResult.Buffer.End);
        }
        while (!readResult.IsCompleted);

        if (context.ParseStack.Count > 0)
        {
            // NOT GOOD - unmatched non-terminals still on the parse stack
            throw new InvalidOperationException();
        }
    }

    private SequencePosition ParseReadResult(in ReadResult readResult, ParseContext context)
    {
        var segmentReader = new SegmentReader(readResult.Buffer);

        while (context.ParseStack.TryPeek(out var symbol))
        {
            switch (symbol)
            {
                case ParseStackSymbol.Segment:
                    {
                        context.ParseStack.Pop();
                        context.ParseStack.Push(ParseStackSymbol.ChangeSetsOrEof);
                        context.ParseStack.Push(ParseStackSymbol.SegmentHeader);
                        break;
                    }

                case ParseStackSymbol.SegmentHeader:
                    {
                        if (!segmentReader.TryReadSegmentHeader(out var minGeneration, out var maxGeneration))
                        {
                            return segmentReader.Position;
                        }

                        context.ParseStack.Pop();
                        break;
                    }

                case ParseStackSymbol.ChangeSetsOrEof:
                    {
                        // If there is no more data in the current chunk and the data source indicates
                        // that the input has been completed, pop the last symbol off the stack so that
                        // parsing will finish gracefully.
                        if (segmentReader.End)
                        {
                            if (readResult.IsCompleted)
                            {
                                context.ParseStack.Pop();
                            }

                            return segmentReader.Position;
                        }

                        // If there is more data in the current chunk, prepare to parse the next change set.
                        context.ParseStack.Push(ParseStackSymbol.ChangeSetHeader);
                        break;
                    }

                case ParseStackSymbol.ChangeSetHeader:
                    {
                        if (!segmentReader.TryReadChangeSetHeader(out var payloadCount, out var tombstoneCount))
                        {
                            return segmentReader.Position;
                        }

                        context.ParseStack.Pop();

                        for (var i = 0; i < payloadCount; i++)
                        {
                            context.ParseStack.Push(ParseStackSymbol.PayloadValue);
                        }

                        for (var i = 0; i < tombstoneCount; i++)
                        {
                            context.ParseStack.Push(ParseStackSymbol.Tombstone);
                        }

                        for (var i = 0; i < payloadCount; i++)
                        {
                            context.ParseStack.Push(ParseStackSymbol.PayloadHeader);
                        }

                        break;
                    }

                case ParseStackSymbol.PayloadHeader:
                    {
                        if (!segmentReader.TryReadPayloadHeader(out var key, out var valueLength))
                        {
                            return segmentReader.Position;
                        }

                        context.ParseStack.Pop();
                        context.Payloads.Enqueue((Key: key, ValueLength: valueLength));

                        break;
                    }

                case ParseStackSymbol.Tombstone:
                    {
                        if (!segmentReader.TryReadTombstone(out var key))
                        {
                            return segmentReader.Position;
                        }

                        context.ParseStack.Pop();
                        break;
                    }

                case ParseStackSymbol.PayloadValue:
                    {
                        var (_, valueLength) = context.Payloads.Peek();

                        if (!segmentReader.TryAdvancePastPayloadValue(valueLength))
                        {
                            return segmentReader.Position;
                        }

                        context.ParseStack.Pop();
                        context.Payloads.Dequeue();
                        break;
                    }

                default:
                    throw new InvalidOperationException();
            }
        }

        return segmentReader.Position;
    }

    private class ParseContext
    {
        public ParseContext()
        {
            ParseStack.Push(ParseStackSymbol.Segment);
        }

        public Stack<ParseStackSymbol> ParseStack { get; } = new();

        public Queue<(byte[] Key, int ValueLength)> Payloads { get; } = new();
    }

    private enum ParseStackSymbol
    {
        Segment = 1,
        SegmentHeader,
        ChangeSetsOrEof,
        ChangeSetHeader,
        PayloadHeader,
        PayloadValue,
        Tombstone,
    }
}