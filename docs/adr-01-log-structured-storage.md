# ADR-01 Log-structured storage

## Status

> What is the status, such as proposed, accepted, rejected, deprecated, superseded, etc.?

Accepted

## Context

> What is the issue that we're seeing that is motivating this decision or change?

To durably store key/value pairs in the underlying file system, we need to define how insertions, updates and deletions interact with the on-disk representation of the key/value pairs.

A design should aim to achieve the following primary qualities:

* Conceptual simplicity
* Update performance (throughput)

We broadly consider the following spectrum of options:

* B+ trees and comparable disk-optimized search trees, in which changes are effected directly to the tree representation on disk.
* B+ trees with a write-ahead log (WAL), in which changes are written sequentially to the WAL and consolidated into the main B+ tree in a background process.
* Append-only ("log-structured") storage schemes, in which changes are written sequentially to a log and the log itself undergoes occasional trimming and compaction.

## Decision

> What is the change that we're proposing and/or doing?

Given the design goals stated above and considering the relatively narrow feature set that Fugu aims for, we decide on a log-structured storage scheme for durable storage in Fugu.

However, this decision necessitates follow-up decisions around index organization, representing deletions, and general housekeeping.

## Consequences

> What becomes easier or more difficult to do because of this change?

* A simple storage scheme will make it easier to reason about correctness of the implementation
* Sequential, append-only writes align well with performance characteristics of modern flash-based storage devices, which we expect to translate to good overall write throughput
* Lack of an embedded on-disk index forces us to define indexing strategy for efficient keyed and/or range-based data retrieval (e.g., via auxilliary index files or in-memory indices)