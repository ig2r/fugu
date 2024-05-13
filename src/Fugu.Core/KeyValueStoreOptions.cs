using Fugu.IO;

namespace Fugu;

public sealed class KeyValueStoreOptions
{
    public required IBackingStorage Storage { get; set; }
}
