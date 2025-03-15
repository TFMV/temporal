# Data Processing Pipeline with Temporal and Apache Arrow

A high-performance data processing pipeline using Temporal for workflow orchestration and Apache Arrow for efficient data handling.

## Features

- **Streaming Record Batch Processing**: Process data in batches for optimal throughput
- **Zero-Copy Operations**: Minimize memory overhead with Arrow's zero-copy operations
- **Vectorized Execution**: Leverage Arrow's columnar format for vectorized processing
- **Fault Tolerance**: Utilize Temporal's reliability features for resilient workflows
- **Memory Efficiency**: Optimize memory usage with Arrow's columnar data structures
- **Scalability**: Scale horizontally with Temporal workers
- **Arrow Flight Integration**: Direct memory sharing between activities using Arrow Flight

## Architecture

The pipeline consists of several key components:

### System Architecture Diagram

```mermaid
graph TD
    classDef temporal fill:#7986CB,stroke:#303F9F,color:white
    classDef arrow fill:#4DB6AC,stroke:#00796B,color:white
    classDef flight fill:#FF8A65,stroke:#D84315,color:white
    classDef storage fill:#9575CD,stroke:#512DA8,color:white
    classDef worker fill:#4FC3F7,stroke:#0288D1,color:white
    
    Client[Client Application] --> Temporal[Temporal Server]
    
    subgraph "Workflow Orchestration"
        Temporal --> WorkflowWorker[Workflow Worker]:::temporal
        WorkflowWorker --> ActivityWorker1[Activity Worker 1]:::worker
        WorkflowWorker --> ActivityWorker2[Activity Worker 2]:::worker
        WorkflowWorker --> ActivityWorker3[Activity Worker 3]:::worker
    end
    
    subgraph "Data Processing"
        ActivityWorker1 --> |Generate Batch| FlightServer[Arrow Flight Server]:::flight
        ActivityWorker2 --> |Get & Process Batch| FlightServer
        ActivityWorker2 --> |Put Processed Batch| FlightServer
        ActivityWorker3 --> |Get & Store Batch| FlightServer
        
        FlightServer --> |Zero-Copy Data Transfer| ArrowMemory[Arrow Memory Format]:::arrow
        ArrowMemory --> |Columnar Data| VectorizedOps[Vectorized Operations]:::arrow
    end
    
    subgraph "Storage & Cleanup"
        FlightServer --> |TTL-based Cleanup| BatchCleanup[Batch Cleanup]:::storage
        ActivityWorker3 --> |Persist Results| DataStore[Data Store]:::storage
    end
    
    style Temporal font-weight:bold
    style FlightServer font-weight:bold
    style ArrowMemory font-weight:bold
```

### Arrow Data Converter

Efficiently serializes and deserializes Arrow data structures for Temporal payloads.

### Arrow Flight Server

Enables direct memory sharing between activities, minimizing serialization overhead.

### Streaming Workflow

Orchestrates the data processing pipeline with Temporal, managing the flow of data between activities.

### Data Processing Activities

- **Generate Batch Activity**: Creates Arrow RecordBatches with sample data
- **Process Batch Activity**: Filters and transforms the data using vectorized operations
- **Store Batch Activity**: Stores the processed data (simulated in this example)

### Batch Processors

Implements vectorized operations on Arrow data for efficient processing.

### Command-line Interface

Provides a flexible interface for configuring and running the pipeline.

## Workflow Orchestration

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Generate Batch │────▶│  Process Batch  │────▶│   Store Batch   │
│    Activity     │     │    Activity     │     │    Activity     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Arrow Flight Server                       │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Details

### Zero-Copy Operations with Arrow Flight

The pipeline uses Arrow Flight for direct memory sharing between activities:

```go
// Store a batch in the Flight server
batchID, err := flightClient.PutBatch(ctx, batch)

// Retrieve a batch from the Flight server
retrievedBatch, err := flightClient.GetBatch(ctx, batchID)
```

### Vectorized Processing

Arrow's columnar format enables efficient vectorized operations:

```go
// Filter rows based on a threshold
filteredBatch, err := arrow.FilterBatch(batch, threshold)
```

## Known Limitations

- **Schema Flexibility**: The current implementation uses a fixed schema
- **Memory Management**: Large datasets may require careful memory management
- **Error Handling**: Error recovery could be improved for production use
- **Data Type Support**: Limited to a subset of Arrow data types
- **Serialization Overhead**: Minimized but not eliminated with Arrow Flight
- **Temporal Payload Size Limits**: Bypassed with Arrow Flight for large datasets
- **Compute Utilization**: Could be further optimized with SIMD instructions
