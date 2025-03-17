# Zero-Copy, Zero-Delay

## Scaling Data Pipelines with Temporal & Arrow Flight

I believe, deeply, that if we can make data movement instantaneous, we change the world.

Data moves. Workflows crash. Scaling stalls out.

We needed a better way; a way to move data without moving it.

By combining Temporal for orchestration and Apache Arrow Flight for high-speed data transfer, we built a pipeline that skips the usual slowdowns. No bloated serialization. No wasteful copies. Just raw, zero-copy data movement forged for the real fight.

Let's dive in.

## Temporal in 60 Seconds

Temporal is a workflow orchestration engine that solves the hardest parts of distributed systems — durability, visibility, reliability, and scalability.

Think of Temporal as an operating system for your distributed applications. You write code that describes what should happen, and Temporal ensures it does — even when things go wrong. No more brittle state machines. No more manual retries. No more losing track of execution when services fail.

And you're not locked into a single language. Temporal supports multiple languages, including Go, Java, TypeScript, and Python, so teams can build workflows using the stack that fits them best.

The magic? Temporal persists the complete state of your workflow, allowing it to resume exactly where it left off after any failure. This makes building resilient distributed applications dramatically simpler — and removes the hidden complexity that derails scalability.

## Apache Arrow Flight in 60 Seconds

Apache Arrow Flight is a high-performance framework designed to solve the hardest parts of large-scale data transfer — latency, serialization overhead, and inefficient data movement.

Think of Arrow Flight as a direct pipeline for data-intensive applications. Instead of forcing data through slow, repetitive serialization steps, Flight moves it exactly as it exists in memory — zero-copy, columnar, and ready for compute.

The result? Transfers that are 5–10x faster than conventional approaches, with dramatically lower memory usage and CPU overhead.

And like Temporal, Arrow Flight isn't locked to a single stack — it integrates seamlessly with multiple languages and frameworks.

If you're processing massive datasets with traditional serialization methods, you're leaving performance on the table.

> If you're processing massive datasets with traditional serialization methods, you're leaving performance on the table.

## The Power of Integration

Temporal excels at orchestrating workflows across distributed systems — managing retries, state, and visibility with ease. But in many data pipelines, large payloads moving between activities become the bottleneck. Serialization overhead creates drag, payload limits introduce constraints, and performance crumbles under the weight of outdated transfer methods.

That's where Apache Arrow Flight takes over. No JSON. No Protobuf. No Avro. Just raw, zero-copy data movement at high speed.

Temporal keeps workflows running smoothly.

Arrow Flight keeps data moving without friction.

One manages orchestration. The other eliminates transfer inefficiencies. No bottlenecks. No wasted cycles. Pure speed.

## Architecture Overview

Here's how it all fits together:

### Flight Orchestration

![Flight Orchestration Diagram](temporal.png)

#### Why This Approach is Special

Here's why this approach stands out:

1. **Memory-Efficient Orchestration**: Traditional data pipelines often suffer from the "double serialization problem" - data is serialized once to pass between services and again to store workflow state. Our approach eliminates this by keeping large data payloads outside the workflow state, passing only lightweight references through Temporal.

2. **Zero-Copy Data Movement**: Unlike conventional approaches that require multiple data copies during transfers, Arrow Flight maintains data in its columnar format throughout the entire pipeline. This means data moves between activities without the overhead of serialization/deserialization cycles, dramatically reducing CPU usage and memory pressure.

3. **Decoupled Scaling**: By separating orchestration concerns (handled by Temporal) from data movement concerns (handled by Arrow Flight), each component can scale independently according to its specific bottlenecks. Need more compute? Scale your workers. Need faster data transfer? Scale your Flight servers.

4. **Resilience Without Compromise**: Temporal provides bulletproof workflow durability and retry semantics, while Arrow Flight ensures data integrity during transfers. This combination delivers enterprise-grade reliability without sacrificing performance - something previously thought impossible in high-throughput data systems.

5. **Vectorized Processing**: Because data remains in Arrow's columnar format throughout the pipeline, activities can leverage modern CPU architectures through SIMD (Single Instruction, Multiple Data) operations. This enables near-hardware-speed processing that traditional row-based approaches simply cannot match.

6. **Language Agnostic**: Both Temporal and Arrow Flight support multiple programming languages, allowing teams to build polyglot data pipelines where each component uses the most appropriate language for its task - Go for orchestration, Python for ML processing, Java for integration with existing systems, etc.

The result is not just an incremental improvement on existing patterns — it’s a fundamental reimagining of how data should flow through distributed systems. By allowing Temporal to focus on orchestration and leveraging Apache Arrow Flight for high-performance data movement, we’ve built a system capable of handling orders of magnitude more data with significantly less infrastructure.

At the core of this architecture, a Temporal Server orchestrates workflows, delegating tasks to Activity Workers, each responsible for a specific stage of the pipeline. Meanwhile, the Arrow Flight Server acts as a high-speed intermediary, enabling workers to push, retrieve, and process data without bottlenecks. Thanks to Apache Arrow’s columnar format, data moves seamlessly through the pipeline with zero-copy transfers and vectorized execution, ensuring maximum efficiency. Processed data is either persisted or automatically cleaned up based on TTL policies.

Instead of dragging massive payloads through the workflow, we pass only lightweight batch IDs. Rather than burdening Temporal with raw data, each activity simply references a stored batch, retrieving it only when needed. This approach keeps Temporal lean, Arrow Flight fast, and data movement frictionless — a trifecta of efficiency that enables truly scalable, high-performance pipelines.

Ready for takeoff? Explore the repo [here](https://github.com/TFMV/temporal).
