using Fugu.Core.IO;

namespace Fugu.Core.Common;

public record IndexEntry(
    Segment Segment,
    PayloadLocator PayloadLocator);
