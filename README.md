# Fugu

Fugu is a lightweight, embedded key-value storage engine for .NET. It aims to provide fast and simple storage for byte-keyed data in services and applications.

## Work in progress ⚠️

This project is just starting out and major parts are not functional, yet. Stay tuned!

## Features

* Log-structured, append-only storage scheme
* Single-level storage design to leverage the OS page cache
* Multi-version concurrency control (MVCC) with snapshot isolation and atomic multi-key writes
* Keys and values are byte strings, up to 32K/2G in length each
* Background compaction to reclaim unused space