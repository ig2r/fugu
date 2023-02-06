using Fugu.Core.Actors.Messages;
using Fugu.Core.IO;
using Fugu.Core.IO.Format;
using System.Threading.Channels;

namespace Fugu.Core.Actors;

public class WriterActor : Actor
{
    private readonly ChannelReader<WriteWriteBatchMessage> _writeWriteBatchChannelReader;
    private readonly ChannelWriter<DummyMessage> _updateIndexChannelWriter;

    private Table? _outputTable;
    private TableWriter? _tableWriter;

    public WriterActor(
        ChannelReader<WriteWriteBatchMessage> writeWriteBatchChannelReader,
        ChannelWriter<DummyMessage> updateIndexChannelWriter)
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

                    // Initialize new output table
                    _outputTable = message.OutputTable;
                    _tableWriter = new TableWriter(_outputTable.BufferWriter);

                    // Write segment header
                    var segmentHeader = new SegmentHeader
                    {
                        FormatVersion = 1,
                        MinGeneration = 1,
                        MaxGeneration = 1,
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
                var pendingValues = new List<Value>(capacity: message.Batch.PendingPuts.Count);

                foreach (var (key, value) in message.Batch.PendingPuts)
                {
                    var commitKeyValue = new CommitKeyValue
                    {
                        KeySize = (short)key.Length,
                        ValueSize = value.Length,
                    };

                    _tableWriter.Write(in commitKeyValue);
                    _tableWriter.WriteBytes(key.Span);

                    pendingValues.Add(value);
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
                foreach (var value in pendingValues)
                {
                    _tableWriter.WriteBytes(value.Span);
                }

                // Write commit trailer
                var commitTrailer = new CommitTrailer
                {
                    Checksum = 42,
                };

                _tableWriter.Write(in commitTrailer);
            }
        }

        // TODO: Writer is shutting down, close out current output table (if any)

        // Input channel has completed, propagate completion
        _updateIndexChannelWriter.Complete();
    }
}
