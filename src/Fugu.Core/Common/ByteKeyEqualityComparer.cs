using System.Diagnostics.CodeAnalysis;

namespace Fugu.Core.Common;

public sealed class ByteKeyEqualityComparer : EqualityComparer<Key>
{
    public override bool Equals(Key x, Key y)
    {
        return x.Span.SequenceEqual(y.Span);
    }

    public override int GetHashCode([DisallowNull] Key obj)
    {
        return obj.Length;
    }
}
