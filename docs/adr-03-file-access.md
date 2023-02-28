# ADR-03 File access

## Status

> What is the status, such as proposed, accepted, rejected, deprecated, superseded, etc.?

Proposed

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

See: [Crotty, A., Leis, V. and Pavlo, A., 2022. Are you sure you want to use mmap in your database management system.](https://db.cs.cmu.edu/mmap-cidr2022/) (CIDR 2022)

## Decision

> What is the change that we're proposing and/or doing?

## Consequences

> What becomes easier or more difficult to do because of this change?
