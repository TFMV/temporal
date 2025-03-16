# Scaling Data Pipelines with Temporal and Apache Arrow Flight

## Mach Speed for Your Data

I believe, deeply, that if we can make data movement instantaneous, we change the world.

Data moves. Workflows crash. Scaling stalls out.

We needed a better way; a way to move data without moving it.

By combining **Temporal** for orchestration and **Apache Arrow Flight** for high-speed data transfer, we built a pipeline that skips the usual slowdowns. No bloated serialization. No wasteful copies. Just raw, efficient movement of data — fast, scalable, and forged for the real fight.

Let's dive in.

## Temporal in 60 Seconds

**Temporal** is a workflow orchestration engine that solves the hardest parts of distributed systems — durability, visibility, reliability, and scalability.

Think of Temporal as an operating system for your distributed applications. You write code that describes what should happen, and Temporal ensures it does — even when things go wrong. No more brittle state machines. No more manual retries. No more losing track of execution when services fail.

And you're not locked into a single language. Temporal supports multiple languages, including:

- Go
- Java
- TypeScript
- Python

So teams can build workflows using the stack that fits them best.

The magic? Temporal persists the complete state of your workflow, allowing it to resume exactly where it left off after any failure. This makes building resilient distributed applications dramatically simpler — and removes the hidden complexity that derails scalability.

## Apache Arrow Flight in 60 Seconds

**Apache Arrow Flight** is a high-performance framework designed to solve the hardest parts of large-scale data transfer — latency, serialization overhead, and inefficient data movement.

Think of Arrow Flight as a direct pipeline for data-intensive applications. Instead of forcing data through slow, repetitive serialization steps, Flight moves it exactly as it exists in memory — zero-copy, columnar, and ready for compute.

The result? Transfers that are **5–10x faster** than conventional approaches, with dramatically lower memory usage and CPU overhead.

And like Temporal, Arrow Flight isn't locked to a single stack — it integrates seamlessly with multiple languages and frameworks.

> If you're processing massive datasets with traditional serialization methods, you're leaving performance on the table.

## The Power of Integration

Temporal excels at orchestrating workflows across distributed systems — managing retries, state, and visibility with ease. But in many data pipelines, large payloads moving between activities become the bottleneck. Serialization overhead creates drag, payload limits introduce constraints, and performance crumbles under the weight of outdated transfer methods.

That's where Apache Arrow Flight takes over:

- No JSON
- No Protobuf
- No Avro

Just raw, zero-copy data movement at high speed.

**Temporal** keeps workflows running smoothly.

**Arrow Flight** keeps data moving without friction.

One manages orchestration. The other eliminates transfer inefficiencies. No bottlenecks. No wasted cycles. Pure speed.

## Architecture Overview

Here's how it all fits together:

### Flight Orchestration

![Flight Orchestration Diagram](flight_orchestration.png)

The diagram illustrates how Temporal and Apache Arrow Flight work together to optimize data movement and processing. Each component plays a distinct role in eliminating bottlenecks and ensuring efficient execution.

1. **Workflow Orchestration** — A Temporal Server orchestrates the workflow, delegating tasks to multiple Activity Workers. Each worker is responsible for a specific stage of the data pipeline.

2. **Data Processing** — The Arrow Flight Server acts as a high-speed intermediary, handling data transfers between activity workers. Workers interact directly with Arrow Flight — pushing, retrieving, and processing data at high speed, without bottlenecks.

3. **Zero-Copy Data Transfer** — Data is stored in Apache Arrow's columnar format, allowing vectorized operations that maximize CPU efficiency.

4. **Storage & Cleanup** — Once data is processed, it's either persisted in storage or automatically cleaned up based on a time-to-live (TTL) policy.

This architecture keeps Temporal focused on orchestration, Arrow Flight handling data movement, and Arrow's in-memory format maximizing processing efficiency — all working together to create a scalable, high-performance pipeline.

## Implementation Details

### The Arrow Flight Server

At the heart of the system is the Arrow Flight Server. Instead of shuffling payloads through the workflow itself, workers push and pull data efficiently — storing batches when needed, retrieving them on demand.

The server speaks the Arrow Flight protocol, handling:

- `DoPut` for writing data
- `DoGet` for retrieval

No unnecessary serialization, no redundant copies—just high-speed, zero-copy transfers between processing stages. This keeps workloads lean, memory overhead low, and performance exactly where it needs to be: fast.

### The Temporal Workflow

Rather than dragging massive payloads through the workflow, we pass only lightweight batch IDs. Instead of flooding Temporal with raw data, each activity references a stored batch, retrieving it only when needed.

1. A **Generate Batch** activity creates data and pushes it to the Arrow Flight Server, returning a batch ID.

2. A **Process Batch** activity pulls the batch, applies transformations, and pushes the result back — again, passing only an ID.

This keeps the workflow lean, efficient, and scalable. Temporal focuses on orchestration, while Arrow Flight handles the heavy lifting of data movement.

As data scales, the efficiency gap only grows wider. Arrow Flight eliminates serialization overhead, reducing processing time and unlocking true high-speed data movement.

## Wrapping Up

We set out to eliminate the turbulence in data pipelines — to move data, not just process it. Temporal steadies workflows with retries and visibility, Arrow Flight keeps data in motion, and together, they clear the runway for true scaling.

No drag. No delays. Just pure acceleration.

Ready for takeoff? [Explore the repo here](https://github.com/TFMV/temporal).
