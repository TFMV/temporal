# High-Performance Data Pipeline with Temporal and Apache Arrow

This project implements a high-performance data processing pipeline using [Temporal](https://temporal.io/) for workflow orchestration and [Apache Arrow](https://arrow.apache.org/) for efficient in-memory data representation and processing. The implementation focuses on optimizing memory usage and leveraging zero-copy operations to handle large datasets efficiently.

## Features

- **Streaming Processing**: Process data in RecordBatches
- **Zero-Copy Operations**: Minimize memory copies during data transfer between activities
- **Vectorized Execution**: Process data in columnar format for better CPU cache utilization and SIMD operations
- **Fault Tolerance**: Leverage Temporal's reliability features for automatic retries and workflow resumption
- **Memory Efficiency**: Control memory usage through batch processing and explicit memory management
- **Scalability**: Distribute processing across multiple workers and handle datasets of any size

## Architecture

The architecture combines Temporal's workflow orchestration capabilities with Apache Arrow's efficient data representation:

### Components

1. **Arrow Data Converter**: Custom Temporal DataConverter that handles Arrow RecordBatch serialization/deserialization
2. **Streaming Workflow**: Temporal workflow that coordinates the processing of data in batches
3. **Data Processing Activities**: Activities that generate, process, and store Arrow RecordBatches
4. **Batch Processors**: Implementations of the BatchProcessor interface for specific data operations
5. **Command-line Interface**: CLI for starting workers and workflows with configurable parameters

### Workflow Orchestration

The streaming workflow orchestrates the data pipeline by:

1. Generating data batches (or reading from a source)
2. Processing each batch through filtering or transformation activities
3. Storing or forwarding the processed batches
4. Tracking progress and handling failures

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Generate   │     │   Process   │     │    Store    │
│    Batch    │────▶│    Batch    │────▶│    Batch    │
│  Activity   │     │  Activity   │     │  Activity   │
└─────────────┘     └─────────────┘     └─────────────┘
        │                  │                   │
        └──────────────────┴───────────────────┘
                           │
                    ┌─────────────┐
                    │  Temporal   │
                    │  Workflow   │
                    └─────────────┘
```

### Data Flow

1. **Data Generation/Ingestion**: Create or read data into Arrow RecordBatches
2. **Serialization**: Convert RecordBatches to Temporal payloads with minimal copying
3. **Processing**: Apply vectorized operations on the columnar data
4. **Deserialization**: Convert Temporal payloads back to RecordBatches with zero-copy where possible
5. **Storage/Output**: Store or forward the processed data

## Implementation Details

### Zero-Copy Operations

The implementation minimizes memory copies by:

- Using Arrow's IPC format for efficient serialization/deserialization
- Retaining Arrow Records to prevent premature release
- Ensuring native endianness to avoid byte swapping
- Explicit memory management with careful `Release()` calls

### Vectorized Processing

Data is processed in a columnar format to leverage:

- Better memory locality and cache utilization
- SIMD instructions for parallel processing
- Reduced iteration overhead

Example of vectorized filtering:

```go
// Process the batch in a vectorized manner
for rowIdx := 0; rowIdx < int(batch.NumRows()); rowIdx++ {
    // Apply the filter: keep rows where value > threshold
    if valueArray.Value(rowIdx) > threshold {
        idBuilder.Append(idArray.Value(rowIdx))
        valueBuilder.Append(valueArray.Value(rowIdx))
        categoryBuilder.Append(categoryArray.Value(rowIdx))
    }
}
```

### Streaming Processing

The implementation:

- Processes data in manageable chunks (RecordBatches)
- Controls memory usage through batch size configuration
- Enables processing of datasets larger than available memory

## Known Limitations

1. **Schema Flexibility**: The current implementation assumes a fixed schema across all batches. Dynamic schema evolution is not fully supported.

2. **Memory Management**: While the implementation aims for efficient memory usage, it still requires careful tuning of batch sizes to avoid out-of-memory errors with very large datasets.

3. **Error Handling**: Error recovery at the batch level is implemented, but partial batch failures may require custom handling depending on the use case.

4. **Data Type Support**: The implementation primarily focuses on common data types (numeric, string). Complex nested types may require additional handling.

5. **Serialization Overhead**: Despite optimizations, there is still some overhead in serializing/deserializing Arrow data for Temporal activities.

6. **Temporal Payload Size Limits**: Very large batches may exceed Temporal's default payload size limits and require configuration adjustments.

7. **Compute Utilization**: While vectorized operations are efficient, the current implementation doesn't fully leverage GPU acceleration or advanced SIMD optimizations.

## Project Structure

```
temporal/
├── cmd/
│   └── pipeline/         # Command-line interface for the pipeline
│       └── main.go       # Entry point for running workers and workflows
├── pkg/
│   ├── arrow/            # Arrow utilities and data converter
│   │   ├── arrow_converter.go  # Custom Temporal DataConverter for Arrow
│   │   └── arrow_utils.go      # Arrow serialization and processing utilities
│   └── workflow/         # Temporal workflows and activities
│       └── streaming_workflow.go  # Implementation of the streaming workflow
└── README.md             # This documentation
```

## Getting Started

### Prerequisites

- Go 1.16 or later
- Temporal server running locally or accessible remotely

### Building

```bash
go build -o pipeline ./cmd/pipeline
```

### Running

Start a worker:

```bash
./pipeline -worker -task-queue arrow-pipeline
```

Start a workflow:

```bash
./pipeline -workflow -task-queue arrow-pipeline -batch-size 1000 -num-batches 10 -threshold 500
```

## Benchmarking

The implementation has been optimized for performance. Key metrics:

- **Memory Usage**: Controlled through batch size configuration
- **Processing Speed**: Improved through vectorized operations
- **Serialization Overhead**: Minimized with Arrow's IPC format

## Performance Optimizations

1. **Pre-allocation**: Memory for builders is pre-allocated based on estimated capacity
2. **Chunk-based Processing**: Data is processed in chunks for better memory locality
3. **Type-aware Processing**: Strong typing is used for better performance
4. **Batch Processing**: Operations are performed on batches to maximize throughput
5. **Memory Reuse**: Builders and arrays are reused where possible to reduce GC pressure

## Extending the Pipeline

To extend the pipeline with new processing capabilities:

1. Implement the `BatchProcessor` interface for your specific operation
2. Create a new activity that uses your processor
3. Update the workflow to include your new activity

Example of a custom batch processor:

```go
type MyCustomProcessor struct {
    // Configuration parameters
}

func (p *MyCustomProcessor) ProcessBatch(batch arrow.Record, config map[string]string) (arrow.Record, error) {
    // Implement your custom processing logic
    return processedBatch, nil
}

func (p *MyCustomProcessor) Release() {
    // Clean up any resources
}
```
