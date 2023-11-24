namespace Fugu.IO;

public static class StorageFormat
{
    // Magic:           8 bytes
    // Major version:   2 bytes
    // Minor version:   2 bytes
    // Min generation:  8 bytes
    // Max generation:  8 bytes
    public const int SegmentHeaderSize = 28;

    // Tag:             1 byte
    // Payload count:   4 bytes
    // Tombstone count: 4 bytes
    public const int ChangeSetHeaderSize = 9;

    // Key length:      4 bytes
    // Value length:    4 bytes
    // (Key follows immediately after)
    public const int PayloadHeaderPrefixSize = 8;

    // Key length:      4 bytes
    // (Key follows immediately after)
    public const int TombstonePrefixSize = 4;
}
