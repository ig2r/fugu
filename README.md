# Fugu

Fugu is a lightweight, log-structured key-value storage engine for .NET.

⚠️ This project is under active development and both API and persistence format could change in compatibility-breaking ways in the future.

## Design goals

- Provide a lightweight in-process storage option for byte array-keyed binary data on all platforms targeted by the CLR (i.e., both Intel and ARM architectures), including AOT compilation.
- Enable atomic multi-key writes in API.
- Implement MVCC scheme with snapshot isolation reads.
- Prioritize good throughput under sustained writes by designing for low allocation and low write amplification.
- Concurrent background compaction.

### Non-goals

- Queries outside of key-based value lookup.
- Network API - Fugu is intended to be run in-process.
- Multiple processes accessing the same files in backing storage simultaneously.
- On-disk indexes - to reduce complexity and increase robustness, the index of values is kept in RAM only. As a consequence, data volumes whose index exceeds available RAM are not supported.

## Usage

```csharp
using Fugu;

// Persistent backing storage for store data
var storage = new InMemoryStorage();

await using var store = await KeyValueStore.CreateAsync(storage);

// Atomic writes - add/update "foo" and "bar", delete value for "baz"
var changeSet = new ChangeSet
{
    ["foo"u8] = new byte[10],
    ["bar"u8] = Array.Empty<byte>(),
};

changeSet.Remove("baz"u8);
await store.SaveAsync(changeSet);

// Reads always go through a snapshot
using (var snapshot = await store.GetSnapshotAsync())
{
    var value = await snapshot.ReadAsync("foo"u8);
}

```

## Architecture

Fugu provides its functionality using three major conceptual building blocks:

- A backing storage interface that deals in opaque binary arrays.
- A mesh of relatively autonomous actors that each implement a specific area of responsibility and communicate through bounded, ring-buffered channels.
- A consumer-facing `KeyValueStore` type that manages store lifecycle and provides a cohesive API over the underlying actor mesh.

### Actors

- **Allocation**: Distributes incoming change sets across slabs in backing storage.
- **Writer**: Writes out each incoming change sets into its assigned slab.
- **Index**: Keeps track of where values are stored in backing storage, and keeps track of overall utilization.
- **Snapshots**: Manages consistent, read-only snapshots of the data.
- **Compaction**: Preserves space/efficiency invariants, e.g., merging segments when their ratio of "unused" data becomes too high.
