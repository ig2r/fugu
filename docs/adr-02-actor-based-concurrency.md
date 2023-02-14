# ADR-02 Actor-based concurrency

## Status

> What is the status, such as proposed, accepted, rejected, deprecated, superseded, etc.?

Accepted

## Context

> What is the issue that we're seeing that is motivating this decision or change?

As described in [ADR-01](adr-01-log-structured-storage.md), we choose to design Fugu around a log-structured approach to storage organization for two primary reasons:

* Conceptual simplicity, and
* Write throughput.

To achieve both, we need to strike a balance by choosing an adequate concurrency model for the implementation.

For instance, consider that under sustained writes, log-structured storage will require periodic compaction runs to reclaim unused space. Stalling writes while compaction happens would severly impact throughput and write latency. However, at the other end of the spectrum, ad-hoc concurrency would introduce a risk of decreased maintainability in the long run.

Additionally, the prevalently multi-core architecture of modern CPUs opens up the possibility of increased job throughput by utilizing compute parallelism. The [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) pattern in particular leverages multi-core platforms through a low-lock design combined with good cache utilization per CPU core. So there is definitely some research and prior art in this area.

## Decision

> What is the change that we're proposing and/or doing?

We decide to model concurrency in Fugu around an actor model, in which actors communicate through asynchronous message passing.

Each actor shall be responsible for a clearly-defined chunk of functionality (e.g., index management, snapshot tracking, etc.) and manage its own state internally. As the "inner loop" of actors will be implemented on a single logical thread, this will remove the need for synchronization when accessing this internal state.

## Consequences

> What becomes easier or more difficult to do because of this change?

* Having a concurrency model at our disposal in the first place lets us shift operations off performance-critical write and read paths, and benefit from multiple CPU cores.
* A comparatively coarse-grained concurrency model allows us to reason about the system's behavior more confidently because we only need to keep interactions at actor boundaries in our head.
* We need to define how the message-passing between actors will be implemented. The LMAX Disruptor technical paper should be instructive w.r.t. performance pitfalls in this regard.
* We need to carefully measure the performance trade-offs of this approach vis-a-vis a straightforward single-threaded implementation.