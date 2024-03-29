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

Fugu consists of three major conceptual building blocks:

- A backing storage interface that deals in opaque binary arrays.
- A mesh of relatively autonomous actors that each implement a specific area of responsibility and communicate through bounded, ring-buffered channels.
- A consumer-facing `KeyValueStore` type that manages store lifecycle and provides a cohesive API over the underlying actor mesh.

### Actors

- **Allocation**: Distributes incoming change sets across slabs in backing storage.
- **Writer**: Writes out each incoming change set into its assigned slab.
- **Index**: Keeps track of positions and sizes of written values in backing storage by key. Maintains statistics of "live" and "stale" bytes for each segment.
- **Snapshots**: Manages consistent, read-only snapshots of the data.
- **Compaction**: Preserves space/efficiency invariants, e.g., merging segments when their ratio of "stale" vs. "live" data grows too high.

The following diagram illustrates messaging paths between actors and the `KeyValueStore` facade.

![Actor mesh](docs/actor-mesh.png)

## Compaction strategy

As with any log-structured persistence scheme, Fugu needs to implement a compaction scheme to ensure that stale data (e.g., values that have been overwritten or deleted) is periodically garbage collected. The remainder of this section provides an outline of this strategy.

- The store is partitioned into a sequence of segments. Only the most recent segment (the *output segment*) accepts writes.
- When the output segment reaches a predefined size limit, writes to it will stop and a new, initially empty output segment is started.
- To put an upper bound on the total number of segments, segment size limits are chosen in such a way that segment sizes follow a geometric series characterized by coefficient $a$ and ratio $r > 1$, where the $i$-th segment is of size $a * r^i$.

  Consider the cumulative sum of a geometric series with $n$ elements and parameters $(a, r)$:

  $$S = a \left( \frac{1 - r^n}{1 - r} \right)$$

  Solving for $n$ gives us the idealized number of segments in terms of the total amount of data $S$:

  $$n = log_r \left( 1 - \frac{S}{a} (1 - r) \right)$$

  This $n$ serves two purposes:

  - It implies the size limit for the next output segment, i.e., $a * r^n$.
  - It can indicate a violation of the geometric series invariant if the actual number of segments is considerably higher than $n$.

- As restoring the invariant is thus mainly a matter of bringing the actual number of segments down to $n$, the compaction algorithm will identify two or more adjacent segments and merge them into one. Note that the current output segment will never be included in a compaction.
- During index updates, the index actor will maintain statistics on the amount of "live" and "stale" data per segment. This information will be a primary input for selecting compaction candidates, as we aim to flush out as much stale data as possible during the compacting merge.

## Segment serialization format

Data within each segment is stored in a format that is designed to be reasonably simple and efficient to parse and write using vectorized I/O. Integers are generally stored in little-endian order.

```
<Segment> ::= <SegmentHeader> <ChangeSet>* (EOF)

<SegmentHeader> ::=
  Magic                   : byte[8]
  Format version (major)  : uint16
  Format version (minor)  : uint16
  Min segment generation  : uint64
  Max segment generation  : uint64

<ChangeSet> ::=
  Payload count           : uint32
  Tombstone count         : uint32
  Payload key lengths     : uint32[]
  Tombstone key lengths   : uint32[]
  Payload value lengths   : uint32[]
  Payload keys            : byte[]
  Tombstone keys          : byte[]
  Payload values          : byte[]
  Running checksum        : uint64
```