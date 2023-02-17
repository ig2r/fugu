# Actor mesh

The following diagram shows the relationship between the client-facing API and main actors in the system.

```mermaid
graph TD

  KeyValueStore("KeyValueStore API")
  Allocation(Allocation actor)
  Writer(Writer actor)
  Index(Index actor)
  Snapshots(Snapshots actor)
  SegmentStats(Segment stats actor)
  Compaction(Compaction actor)

  KeyValueStore -- submit --> Allocation
  KeyValueStore -- acquire --> Snapshots
  KeyValueStore -- wait --> Snapshots

  Allocation -- write --> Writer

  Writer -- updateIndex --> Index

  Index -. indexUpdated .-> Snapshots
  Index -- updateStats --> SegmentStats

  Snapshots -. snapshotsUpdated .-> Compaction

  SegmentStats -. statsUpdated ..-> Compaction
  SegmentStats -- segmentEmptied --> Compaction

  Compaction -- updateIndex --> Index
  Compaction -- segmentEvicted --> Allocation
```

### Legend
* Solid lines: all messages delivered individually
* Dotted lines: only latest message delivered