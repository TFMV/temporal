# High-Performance Data Pipeline with Temporal and Apache Arrow

This project implements a high-performance data processing pipeline that combines [Temporal](https://temporal.io/) for reliable workflow orchestration with [Apache Arrow](https://arrow.apache.org/) for efficient in-memory data representation. The implementation focuses on optimizing memory usage, leveraging zero-copy operations, and utilizing gRPC for efficient data transfer between workflow activities.

## Key Features

- **Streaming Record Batch Processing**: Process data in manageable chunks (RecordBatches) rather than loading entire datasets into memory
- **Zero-Copy Operations**: Minimize memory copies during data transfer between activities
- **Vectorized Execution**: Process data in columnar format for better CPU cache utilization and SIMD operations
- **Optimized gRPC Transport**: Custom gRPC configuration for high-throughput data transfer
- **Automatic Chunking**: Intelligent splitting of large batches to avoid gRPC message size limits
- **Fault Tolerance**: Temporal's reliability features for automatic retries and workflow resumption
- **Progress Tracking**: Heartbeat mechanism to track progress of long-running activities

## Architecture

The architecture combines Temporal's workflow orchestration capabilities with Apache Arrow's efficient data representation:

### Components

1. **Arrow Data Converter**: Custom Temporal DataConverter that handles Arrow RecordBatch serialization/deserialization
2. **Streaming Workflow**: Temporal workflow that coordinates the processing of data in batches
3. **Data Processing Activities**: Activities that generate, process, and store Arrow RecordBatches
4. **Batch Processors**: Implementations of the BatchProcessor interface for specific data operations
5. **Optimized gRPC Transport**: Custom gRPC configuration for efficient data transfer
6. **Command-line Interface**: CLI for starting workers and workflows with configurable parameters

### Workflow Orchestration

The streaming workflow orchestrates the data pipeline by:

1. Generating data batches (or reading from a source)
2. Automatically splitting large batches into manageable chunks
3. Processing each batch/chunk through filtering or transformation activities
4. Storing or forwarding the processed batches
5. Tracking progress and handling failures

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
2. **Batch Size Evaluation**: Check if batches need to be split into smaller chunks
3. **Processing**: Apply vectorized operations on the columnar data
4. **Storage/Output**: Store or forward the processed data

## Implementation Details

### Zero-Copy Operations

The implementation minimizes memory copies by:

- Using Arrow's IPC format for efficient serialization/deserialization
- Ensuring native endianness to avoid byte swapping

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

### gRPC Transport

The implementation uses gRPC configuration to optimize data transfer:

- Keepalive parameters to maintain connection stability
- Connection pooling for better resource utilization

```go
// Set up gRPC options
return []grpc.DialOption{
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithKeepaliveParams(kaParams),
    grpc.WithDefaultCallOptions(
        grpc.MaxCallRecvMsgSize(64 * 1024 * 1024), // 64MB max message size
        grpc.MaxCallSendMsgSize(64 * 1024 * 1024), // 64MB max message size
    ),
}
```

### Automatic Chunking

To handle large datasets efficiently, the workflow automatically splits large batches into smaller chunks:

```go
// Check if batch is too large for efficient gRPC transport
if int(batch.NumRows()) > defaultMaxBatchSize {
    logger.Info("Batch is too large, splitting into smaller chunks", 
        "batchNumber", i, 
        "rows", batch.NumRows(), 
        "maxBatchSize", defaultMaxBatchSize)
    
    // Process the batch in chunks
    processedCount += processBatchInChunks(ctx, batch, params.Threshold, i)
}
```

### Progress Tracking

For long-running activities, the implementation uses Temporal's heartbeat mechanism:

```go
// Heartbeat periodically for large batches
if i > 0 && i%10000 == 0 {
    activity.RecordHeartbeat(ctx, i)
}
```

## Testing

The project includes tests, with special handling for Temporal-dependent tests.

### Running Tests

To run all tests:

```bash
go test -v ./...
```

By default, Temporal-dependent tests are skipped to allow testing without a running Temporal server. To enable Temporal tests:

```bash
ENABLE_TEMPORAL_TESTS=true go test -v ./...
```

### Test Categories

1. **Arrow Data Processing Tests**: Tests for Arrow serialization, deserialization, and data processing functions
   - These tests run without requiring a Temporal server
   - Focus on verifying data integrity and processing correctness

2. **Temporal Workflow Tests**: Tests for workflow and activity implementations
   - These tests are skipped by default unless `ENABLE_TEMPORAL_TESTS=true` is set
   - Require a running Temporal server when enabled

3. **Configuration Tests**: Tests for command-line and configuration handling
   - Verify proper loading of configuration from different sources (environment variables, command-line flags, config files)

### Test Implementation Details

The tests use several techniques to ensure robustness:

1. **Behavior Verification**: Tests focus on verifying the behavior of functions rather than implementation details
   - For example, filtering tests verify that all values in the filtered batch meet the threshold criteria

2. **Memory Management**: Tests include proper resource cleanup to avoid memory leaks
   - Arrow objects are properly released after use

3. **Mocking**: Temporal workflow tests use mocking to avoid actual execution of activities
   - This allows testing workflow logic without external dependencies

4. **Conditional Execution**: Tests use environment variables to conditionally skip Temporal-dependent tests
   - This allows running the test suite without a Temporal server

## Known Limitations

1. **Schema Flexibility**: The current implementation assumes a fixed schema across all batches. Dynamic schema evolution is not fully supported.

2. **Memory Management**: While the implementation aims for efficient memory usage, it still requires careful tuning of batch sizes to avoid out-of-memory errors with very large datasets.

3. **Error Handling**: The implementation includes basic error handling with retries, but partial batch failures may require custom handling depending on the use case.

4. **Data Type Support**: The implementation primarily focuses on common data types (numeric, string). Complex nested types may require additional handling.

5. **Serialization Overhead**: Despite optimizations, there is still some overhead in serializing/deserializing Arrow data for Temporal activities.

6. **Temporal Payload Size Limits**: Very large batches may exceed Temporal's default payload size limits. The implementation addresses this with increased gRPC message size limits and automatic chunking, but there are still practical upper bounds.

7. **Compute Utilization**: While vectorized operations are efficient, the current implementation doesn't fully leverage GPU acceleration or advanced SIMD optimizations.

8. **Security**: The current gRPC implementation uses insecure credentials for simplicity. Production deployments should use proper TLS credentials.

9. **Chunk Extraction Efficiency**: The current chunk extraction approach processes the entire batch but only keeps rows in the specified range. A more efficient implementation would extract only the needed rows from the batch.

10. **Worker Resource Management**: The implementation doesn't include advanced worker resource management. In production, you might need to implement more sophisticated resource allocation strategies.

## Performance Optimizations

1. **Pre-allocation**: Memory for builders is pre-allocated based on estimated capacity
2. **Chunk-based Processing**: Data is processed in chunks for better memory locality
3. **Type-aware Processing**: Strong typing is used for better performance
4. **Batch Processing**: Operations are performed on batches to maximize throughput
5. **Memory Reuse**: Builders and arrays are reused where possible to reduce GC pressure
6. **gRPC Optimization**: Custom gRPC configuration for efficient data transfer
7. **Worker Configuration**: Optimized worker settings for handling large data volumes
8. **Automatic Chunking**: Intelligent splitting of large batches to avoid gRPC message size limits

## Future Improvements

1. **Dynamic Schema Support**: Enhance the implementation to handle evolving schemas
2. **GPU Acceleration**: Integrate with Arrow CUDA for GPU-accelerated processing
3. **Advanced Monitoring**: Add detailed metrics and monitoring for performance analysis
4. **Resource-aware Scheduling**: Implement resource-aware scheduling for better utilization
5. **Compression**: Add support for compression to reduce network transfer sizes
6. **Security Enhancements**: Implement proper TLS and authentication for production use
7. **More Efficient Chunk Extraction**: Optimize the chunk extraction process to avoid processing the entire batch
8. **Parallel Chunk Processing**: Process chunks in parallel for better throughput
9. **Improved Test Coverage**: Expand test coverage for edge cases and failure scenarios
10. **Integration Tests**: Add integration tests with a real Temporal server in CI/CD pipeline

## Getting Started

### Prerequisites

- Go 1.18 or later
- Temporal server (for running workflows)

### Building

```bash
go build -o arrow-pipeline ./cmd/pipeline
```

### Running

Start a worker:

```bash
./arrow-pipeline --worker --task-queue=arrow-pipeline
```

Start a workflow:

```bash
./arrow-pipeline --workflow --task-queue=arrow-pipeline --batch-size=1000 --num-batches=10 --threshold=500
```

### Configuration

Configuration can be provided through:

1. Command-line flags
2. Environment variables (prefixed with `ARROW_PIPELINE_`)
3. Configuration file (YAML format)

Example configuration file:

```yaml
namespace: default
task-queue: arrow-pipeline
batch-size: 1000
num-batches: 10
threshold: 500
workers: 5
```
