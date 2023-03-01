# ADR-03 File access

## Status

> What is the status, such as proposed, accepted, rejected, deprecated, superseded, etc.?

Accepted

## Context

> What is the issue that we're seeing that is motivating this decision or change?

Fugu needs to interact with the OS file system for persistence. Therefore, we need to decide on an approach to write and read data to/from disk given the following primary design goals:

* Conceptual simplicity,
* Write throughput.

Due to LSM model selected in [ADR-01](adr-01-log-structured-storage.md), we expect write throughput of the system to hinge on good sequential append performance specifically, as opposed to the random access patterns that are typical of B+ trees.

We identify the following high-level options:

* Synchronous file I/O,
* Asynchronous file I/O,
* Memory-mapped files.

Synchronous and asynchronous standard file I/O are quite similar from a programming model perspective. However, synchronous I/O will generally stall the calling thread to wait for I/O completion, requiring any compute workloads to be continued on a different thread.

For asynchronous I/O in .NET 6, it's worth noting that async file access methods under Linux will still defer to blocked threads, pending implementation of io_uring support.

Memory-mapped file seem to present a convenient file access mechanism in which data can be accessed directly from the OS' page cache without extra copies. However, this comes at the cost of hard-to-mitigate I/O stalls when access to a page triggers a page fault. Crotty et al. observe a number of problems in practical implementations for memory-mapped databases.

See: [Crotty, A., Leis, V. and Pavlo, A., 2022. Are you sure you want to use mmap in your database management system.](https://db.cs.cmu.edu/mmap-cidr2022/) (CIDR 2022)

## Decision

> What is the change that we're proposing and/or doing?

We choose **asynchronous file I/O** as Fugu's mechanism to interface with persistent storage. While conceptually alluring, we heed the warning of Crotty et al. regarding observed roadblocks in practice when implementing databases on top of memory-mapped files.

Furthermore, asynchronous file I/O appears to be preferable over synchronous I/O due to Fugu's inherently async makeup, in which asynchronous file I/O should let us avoid thread stalls.

## Consequences

> What becomes easier or more difficult to do because of this change?

* We expect that asynchronous file I/O will complement the compute-async actor model in Fugu and give acceptable throughput for sequential writes, sequential reads, and random-access reads.
* For now, we will rely on the OS' page cache in lieu of a custom-implemented buffer pool. We shall observe if this lets us achieve good performance across relevant scenarios, and decide on impelementing a buffer pool once these measurements are in.