using System.Runtime.InteropServices;

namespace Fugu.Storage.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly record struct ChangeSetHeader(
    FormatTag Tag,
    int ValueCount,
    int TombstoneCount);
