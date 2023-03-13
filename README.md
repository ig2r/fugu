# Fugu

Fugu is a lightweight, embedded key-value storage engine for .NET. It aims to provide fast and simple storage for byte-keyed data in services and applications.

## Work in progress ⚠️

This project is just starting out and major parts are not functional, yet. Stay tuned!

## Features

* Log-structured, append-only storage scheme
* Multi-version concurrency control (MVCC) with snapshot isolation and atomic multi-key writes
* Keys and values are byte strings, up to 32K/2G in length each
* Background compaction to reclaim unused space

## Example

```csharp
using Fugu.Core;

// Keys and values are plain byte strings
byte[] DEMO_KEY = { 1, 2, 3 };
byte[] DEMO_VALUE = { /* ... */ };

// A TableSet represents backing storage for the key-value store
var tableSet = new InMemoryTableSet();

await using (var store = await KeyValueStore.CreateAsync(tableSet))
{
    // Write and remove values transactionally through WriteBatch objects
    var batch = new WriteBatch();
    batch[DEMO_KEY] = DEMO_VALUE;
    await store.WriteAsync(batch);

    // Create a snapshot to obtain a consistent view of the store's contents
    using (var snapshot = await store.GetSnapshotAsync())
    {
        if (snapshot.TryGetLength(DEMO_KEY, out var length))
        {
            var buffer = new byte[length];
            await snapshot.ReadAsync(DEMO_KEY, buffer);
        }
    }
}
```
