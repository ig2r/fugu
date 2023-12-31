﻿using System.Runtime.InteropServices;

namespace Fugu.Core.IO.Format;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct SegmentTrailer
{
    public RecordDiscriminator Discriminator { get; init; }
}
