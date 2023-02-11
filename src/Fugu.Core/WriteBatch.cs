using Fugu.Core.Common;

namespace Fugu.Core;

public class WriteBatch
{
    private readonly Dictionary<Key, Value> _pendingPuts = new(new ByteKeyEqualityComparer());
    private readonly HashSet<Key> _pendingRemovals = new(new ByteKeyEqualityComparer());

    public IReadOnlyDictionary<Key, Value> PendingPuts => _pendingPuts;
    public IReadOnlySet<Key> PendingRemovals => _pendingRemovals;

    public Value this[Key key]
    {
        set
        {
            _pendingPuts[key] = value;
            _pendingRemovals.Remove(key);
        }
    }

    public void Remove(Key key)
    {
        _pendingPuts.Remove(key);
        _pendingRemovals.Add(key);
    }
}
