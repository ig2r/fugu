using System.Runtime.InteropServices;

namespace Fugu.Core.IO.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct CommitHeader
{
    public int Count { get; init; }
}
