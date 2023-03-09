using Fugu.Core.Actors.Messages;
using Fugu.Core.Common;
using Fugu.Core.IO;
using Fugu.Core.IO.Format;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class WriterActor : Actor
{
    private readonly ChannelReader<WriteWriteBatchMessage> _writeWriteBatchChannelReader;
    private readonly ChannelWriter<UpdateIndexMessage> _updateIndexChannelWriter;

    private WritableTable? _outputTable;
    private TableWriter? _tableWriter;
    private Segment? _outputSegment;

    public WriterActor(
        ChannelReader<WriteWriteBatchMessage> writeWriteBatchChannelReader,
        ChannelWriter<UpdateIndexMessage> updateIndexChannelWriter)
    {
        _writeWriteBatchChannelReader = writeWriteBatchChannelReader;
        _updateIndexChannelWriter = updateIndexChannelWriter;
    }

    public override Task RunAsync()
    {
        return HandleWriteWriteBatchMessagesAsync();
    }

    private async Task HandleWriteWriteBatchMessagesAsync()
    {
        while (await _writeWriteBatchChannelReader.WaitToReadAsync())
        {
            if (_writeWriteBatchChannelReader.TryRead(out var message))
            {
                // If output switches to a different table, write trailers and headers
                if (_outputTable != message.OutputTable)
                {
                    // Close out previous output table
                    if (_outputTable is not null)
                    {
                        var segmentTrailer = new SegmentTrailer
                        {
                            Discriminator = RecordDiscriminator.SegmentTrailer,
                        };

                        _tableWriter!.Write(in segmentTrailer);
                    }

                    // Determine generation of the new output segment
                    var outputGeneration = _outputSegment?.MaxGeneration + 1 ?? 1;

                    // Initialize new output segment backed by the current output table
                    _outputTable = message.OutputTable;
                    _tableWriter = new TableWriter(_outputTable.Writer);

                    _outputSegment = new Segment(outputGeneration, outputGeneration, _outputTable);

                    // Write segment header
                    var segmentHeader = new SegmentHeader
                    {
                        FormatVersion = 1,
                        MinGeneration = _outputSegment.MinGeneration,
                        MaxGeneration = _outputSegment.MaxGeneration,
                    };

                    _tableWriter.Write(in segmentHeader);
                }

                var commitHeader = new CommitHeader
                {
                    Discriminator = RecordDiscriminator.CommitHeader,
                    Count = message.Batch.PendingPuts.Count + message.Batch.PendingRemovals.Count,
                };

                _tableWriter!.Write(in commitHeader);

                // Write puts
                var pendingValues = new List<KeyValuePair<Key, Value>>(capacity: message.Batch.PendingPuts.Count);

                foreach (var kvp in message.Batch.PendingPuts)
                {
                    var (key, value) = kvp;

                    var commitKeyValue = new CommitKeyValue
                    {
                        KeySize = (short)key.Length,
                        ValueSize = value.Length,
                    };

                    _tableWriter.Write(in commitKeyValue);
                    _tableWriter.WriteBytes(key.Span);

                    pendingValues.Add(kvp);
                }

                // Write tombstones
                foreach (var key in message.Batch.PendingRemovals)
                {
                    var commitTombstone = new CommitTombstone
                    {
                        KeySize = (short)key.Length,
                    };

                    _tableWriter.Write(in commitTombstone);
                    _tableWriter.WriteBytes(key.Span);
                }

                // Write payload values
                var payloads = new Dictionary<Key, PayloadLocator>();
                foreach (var (key, value) in pendingValues)
                {
                    var position = _tableWriter.Position;
                    _tableWriter.WriteBytes(value.Span);

                    payloads[key] = new PayloadLocator
                    {
                        Start = position,
                        Size = value.Span.Length,
                    };
                }

                // Write commit trailer
                var commitTrailer = new CommitTrailer
                {
                    Checksum = 42,
                };

                _tableWriter.Write(in commitTrailer);

                // Flush commit
                var flushResult = await _outputTable.Writer.FlushAsync();

                // Tell index actor about this write
                await _updateIndexChannelWriter.WriteAsync(
                    new UpdateIndexMessage
                    {
                        Clock = message.Clock,
                        Segment = _outputSegment!,
                        Payloads = payloads,
                        Removals = message.Batch.PendingRemovals,
                    });
            }
        }

        // TODO: Writer is shutting down, close out current output table (if any)

        // Input channel has completed, propagate completion
        _updateIndexChannelWriter.Complete();
    }
}
