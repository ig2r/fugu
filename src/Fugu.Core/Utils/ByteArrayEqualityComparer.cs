using System.Diagnostics.CodeAnalysis;

namespace Fugu.Utils;

internal sealed class ByteArrayEqualityComparer : EqualityComparer<byte[]>
{
    public static ByteArrayEqualityComparer Shared => new();

    public override bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null)
        {
            return false;
        }

        return x.AsSpan().SequenceEqual(y.AsSpan());
    }

    public override int GetHashCode([DisallowNull] byte[] obj)
    {
        // TODO: Improve this
        return obj.Length;
    }
}
