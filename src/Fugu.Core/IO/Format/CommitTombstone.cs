using System.Runtime.InteropServices;

namespace Fugu.Core.IO.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct CommitTombstone
{
    public short KeySize { get; init; }

    // Key follows immediately after
}