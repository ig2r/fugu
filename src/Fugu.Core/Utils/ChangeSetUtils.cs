namespace Fugu.Utils;

internal static class ChangeSetUtils
{
    public static long GetDataBytes(ChangeSet changeSet)
    {
        return changeSet.Payloads.Sum(p => p.Key.Length + p.Value.Length) + changeSet.Tombstones.Sum(t => t.Length);
    }
}
