using System.Runtime.InteropServices;

namespace Fugu.Core.IO.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct SegmentHeader
{
    public int FormatVersion { get; init; }
    public long MinGeneration { get; init; }
    public long MaxGeneration { get; init; }
}
