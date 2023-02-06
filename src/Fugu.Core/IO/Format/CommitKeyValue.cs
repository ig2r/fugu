using System.Runtime.InteropServices;

namespace Fugu.Core.IO.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct CommitKeyValue
{
    public short KeySize { get; init; }
    public int ValueSize { get; init; }

    // Key follows immediately after, values all come in second part of commit
}