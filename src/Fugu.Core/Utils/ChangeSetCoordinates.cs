namespace Fugu.Utils;

/// <summary>
/// Represents payloads and tombstones of a change set that has been persisted
/// to a segment in backing storage.
/// </summary>
/// <param name="Payloads">Payload keys and value offsets/lengths in the associated segment.</param>
/// <param name="Tombstones">Tombstone keys.</param>
public readonly record struct ChangeSetCoordinates(
	IReadOnlyList<KeyValuePair<byte[], SlabSubrange>> Payloads,
	IReadOnlyList<byte[]> Tombstones
);
