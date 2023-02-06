namespace Fugu.Core.IO.Format;

public enum RecordDiscriminator : byte
{
    CommitHeader = 1,
    SegmentTrailer = 2,
}
