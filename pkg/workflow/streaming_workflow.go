// DEPRECATED: This workflow implementation is deprecated in favor of the Flight-based implementation.
// See flight_workflow.go for the new implementation.

package workflow

import (
	"context"
	"fmt"
	"time"

	arrowpkg "github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/TFMV/temporal/pkg/arrow"
)

const (
	// Default activity timeouts
	defaultStartToCloseTimeout    = 10 * time.Minute
	defaultScheduleToStartTimeout = 1 * time.Minute
	defaultHeartbeatTimeout       = 30 * time.Second

	// Default retry policy
	defaultMaxAttempts        = 3
	defaultInitialInterval    = 1 * time.Second
	defaultMaximumInterval    = 1 * time.Minute
	defaultBackoffCoefficient = 2.0

	// Default batch processing parameters
	defaultMaxBatchSize = 50000 // Maximum rows per batch to avoid gRPC message size limits
)

// StreamingWorkflowParams contains all parameters for the streaming workflow
type StreamingWorkflowParams struct {
	BatchSize  int     `json:"batchSize"`
	NumBatches int     `json:"numBatches"`
	Threshold  float64 `json:"threshold"`
	// Add more parameters as needed
}

// StreamingWorkflow demonstrates processing Arrow data in a streaming fashion
// using RecordBatches directly to minimize memory usage and maximize performance
func StreamingWorkflow(ctx workflow.Context, params StreamingWorkflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting streaming Arrow workflow",
		"batchSize", params.BatchSize,
		"numBatches", params.NumBatches,
		"threshold", params.Threshold)

	// Activity options for data processing activities with optimized settings for gRPC
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout:    defaultStartToCloseTimeout,
		ScheduleToStartTimeout: defaultScheduleToStartTimeout,
		HeartbeatTimeout:       defaultHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    defaultInitialInterval,
			MaximumInterval:    defaultMaximumInterval,
			BackoffCoefficient: defaultBackoffCoefficient,
			MaximumAttempts:    defaultMaxAttempts,
			NonRetryableErrorTypes: []string{
				"InvalidArgument",
				"SchemaValidationError",
			},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Process batches in a streaming fashion
	var processedCount int
	for i := 0; i < params.NumBatches; i++ {
		// Generate a batch of data
		var batch arrowpkg.Record
		err := workflow.ExecuteActivity(ctx, GenerateBatchActivity, params.BatchSize, i*params.BatchSize).Get(ctx, &batch)
		if err != nil {
			return fmt.Errorf("failed to generate batch %d: %w", i, err)
		}

		// Check if batch is too large for efficient gRPC transport
		if int(batch.NumRows()) > defaultMaxBatchSize {
			logger.Info("Batch is too large, splitting into smaller chunks",
				"batchNumber", i,
				"rows", batch.NumRows(),
				"maxBatchSize", defaultMaxBatchSize)

			// Process the batch in chunks
			processedCount += processBatchInChunks(ctx, batch, params.Threshold, i)
		} else {
			// Process the batch normally
			var processedBatch arrowpkg.Record
			err = workflow.ExecuteActivity(ctx, ProcessBatchActivity, batch, params.Threshold).Get(ctx, &processedBatch)
			if err != nil {
				return fmt.Errorf("failed to process batch %d: %w", i, err)
			}

			// Store the processed batch
			var storedCount int
			err = workflow.ExecuteActivity(ctx, StoreBatchActivity, processedBatch, fmt.Sprintf("batch_%d", i)).Get(ctx, &storedCount)
			if err != nil {
				return fmt.Errorf("failed to store batch %d: %w", i, err)
			}

			processedCount += storedCount

			// Log progress
			logger.Info("Processed batch", "batchNumber", i, "rowsProcessed", storedCount)
		}
	}

	logger.Info("Completed streaming workflow", "totalRowsProcessed", processedCount)
	return nil
}

// processBatchInChunks processes a large batch by splitting it into smaller chunks
// This helps avoid gRPC message size limits
func processBatchInChunks(ctx workflow.Context, batch arrowpkg.Record, threshold float64, batchNumber int) int {
	logger := workflow.GetLogger(ctx)

	// Calculate number of chunks needed
	totalRows := int(batch.NumRows())
	numChunks := (totalRows + defaultMaxBatchSize - 1) / defaultMaxBatchSize // Ceiling division

	logger.Info("Processing batch in chunks",
		"batchNumber", batchNumber,
		"totalRows", totalRows,
		"numChunks", numChunks)

	// Process each chunk
	var totalProcessed int
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		// Calculate chunk range
		startRow := chunkIdx * defaultMaxBatchSize
		endRow := min(startRow+defaultMaxBatchSize, totalRows)

		// Extract chunk (in a real implementation, this would be more efficient)
		chunkID := fmt.Sprintf("batch_%d_chunk_%d", batchNumber, chunkIdx)

		// Execute chunk processing activity
		var processedChunk arrowpkg.Record
		err := workflow.ExecuteActivity(ctx, ProcessBatchChunkActivity, batch, threshold, startRow, endRow).Get(ctx, &processedChunk)
		if err != nil {
			logger.Error("Failed to process chunk",
				"chunkID", chunkID,
				"error", err)
			continue // Skip this chunk but continue processing others
		}

		// Store the processed chunk
		var storedCount int
		err = workflow.ExecuteActivity(ctx, StoreBatchActivity, processedChunk, chunkID).Get(ctx, &storedCount)
		if err != nil {
			logger.Error("Failed to store chunk",
				"chunkID", chunkID,
				"error", err)
			continue
		}

		totalProcessed += storedCount

		// Log progress
		logger.Info("Processed chunk",
			"batchNumber", batchNumber,
			"chunkIndex", chunkIdx,
			"rowsProcessed", storedCount)
	}

	// Release the original batch
	batch.Release()

	return totalProcessed
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GenerateBatchActivity generates a sample Arrow RecordBatch
func GenerateBatchActivity(ctx context.Context, batchSize int, offset int) (arrowpkg.Record, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Generating batch", "batchSize", batchSize, "offset", offset)

	// Heartbeat to indicate progress for large batches
	activity.RecordHeartbeat(ctx)

	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Define schema for the sample record
	schema := arrowpkg.NewSchema(
		[]arrowpkg.Field{
			{Name: "id", Type: arrowpkg.PrimitiveTypes.Int64},
			{Name: "value", Type: arrowpkg.PrimitiveTypes.Float64},
			{Name: "category", Type: arrowpkg.BinaryTypes.String},
		},
		nil,
	)

	// Create builders for each column
	idBuilder := array.NewInt64Builder(pool)
	defer idBuilder.Release()
	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()
	categoryBuilder := array.NewStringBuilder(pool)
	defer categoryBuilder.Release()

	// Pre-allocate memory for better performance
	idBuilder.Reserve(batchSize)
	valueBuilder.Reserve(batchSize)
	categoryBuilder.Reserve(batchSize * 10) // Estimate average string length

	// Add data to the builders
	for i := 0; i < batchSize; i++ {
		// Heartbeat periodically for large batches
		if i > 0 && i%10000 == 0 {
			activity.RecordHeartbeat(ctx, i)
		}

		idBuilder.Append(int64(offset + i))
		valueBuilder.Append(float64(offset+i) * 1.5)
		categoryBuilder.Append(fmt.Sprintf("category-%d", (offset+i)%5))
	}

	// Build the arrays from the builders
	idArray := idBuilder.NewArray()
	defer idArray.Release()
	valueArray := valueBuilder.NewArray()
	defer valueArray.Release()
	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()

	// Create a record batch from the arrays
	batch := array.NewRecord(schema, []arrowpkg.Array{idArray, valueArray, categoryArray}, int64(batchSize))

	// Note: We don't release the batch here because the caller is responsible for releasing it
	return batch, nil
}

// ProcessBatchActivity filters an Arrow RecordBatch based on a threshold
func ProcessBatchActivity(ctx context.Context, batch arrowpkg.Record, threshold float64) (arrowpkg.Record, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing batch", "numRows", batch.NumRows(), "threshold", threshold)

	// Heartbeat to indicate progress for large batches
	activity.RecordHeartbeat(ctx)

	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Process the batch using the FilterRecordBatch function
	filteredBatch, err := arrow.FilterRecordBatch(batch, threshold, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to filter batch: %w", err)
	}

	// Release the input batch as we no longer need it
	batch.Release()

	logger.Info("Filtered batch", "originalRows", batch.NumRows(), "filteredRows", filteredBatch.NumRows())
	return filteredBatch, nil
}

// ProcessBatchChunkActivity processes a chunk of a larger batch
func ProcessBatchChunkActivity(ctx context.Context, batch arrowpkg.Record, threshold float64, startRow, endRow int) (arrowpkg.Record, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing batch chunk",
		"totalRows", batch.NumRows(),
		"startRow", startRow,
		"endRow", endRow,
		"threshold", threshold)

	// Heartbeat to indicate progress
	activity.RecordHeartbeat(ctx)

	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// In a real implementation, we would extract just the chunk from the batch
	// For this example, we'll process the whole batch but only keep rows in the chunk range

	// Get the schema from the input batch
	schema := batch.Schema()

	// Create builders for the filtered data
	idBuilder := array.NewInt64Builder(pool)
	defer idBuilder.Release()
	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()
	categoryBuilder := array.NewStringBuilder(pool)
	defer categoryBuilder.Release()

	// Pre-allocate memory for better performance
	chunkSize := endRow - startRow
	idBuilder.Reserve(chunkSize)
	valueBuilder.Reserve(chunkSize)
	categoryBuilder.Reserve(chunkSize * 10) // Estimate for string length

	// Get typed arrays for each column
	idArray, idOk := batch.Column(0).(*array.Int64)
	valueArray, valueOk := batch.Column(1).(*array.Float64)
	categoryArray, categoryOk := batch.Column(2).(*array.String)

	// Skip if any type assertion failed
	if !idOk || !valueOk || !categoryOk {
		return nil, fmt.Errorf("failed to type assert columns")
	}

	// Process only the rows in the chunk range
	for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
		// Heartbeat periodically for large chunks
		if rowIdx > startRow && (rowIdx-startRow)%10000 == 0 {
			activity.RecordHeartbeat(ctx, rowIdx-startRow)
		}

		// Apply the filter: keep rows where value > threshold
		if valueArray.Value(rowIdx) > threshold {
			idBuilder.Append(idArray.Value(rowIdx))
			valueBuilder.Append(valueArray.Value(rowIdx))
			categoryBuilder.Append(categoryArray.Value(rowIdx))
		}
	}

	// Build the arrays for the filtered data
	filteredIdArray := idBuilder.NewArray()
	defer filteredIdArray.Release()
	filteredValueArray := valueBuilder.NewArray()
	defer filteredValueArray.Release()
	filteredCategoryArray := categoryBuilder.NewArray()
	defer filteredCategoryArray.Release()

	// Create a record batch with the filtered data
	filteredBatch := array.NewRecord(
		schema,
		[]arrowpkg.Array{filteredIdArray, filteredValueArray, filteredCategoryArray},
		int64(idBuilder.Len()))

	logger.Info("Filtered chunk",
		"chunkSize", chunkSize,
		"filteredRows", filteredBatch.NumRows())

	return filteredBatch, nil
}

// StoreBatchActivity stores an Arrow RecordBatch
func StoreBatchActivity(ctx context.Context, batch arrowpkg.Record, batchID string) (int, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Storing batch", "batchID", batchID, "numRows", batch.NumRows())

	// Heartbeat to indicate progress for large batches
	activity.RecordHeartbeat(ctx)

	// In a real implementation, this would store the batch to a database or file
	// For this example, we'll just count the rows and release the batch

	rowCount := int(batch.NumRows())

	// Release the batch as we no longer need it
	batch.Release()

	logger.Info("Stored batch", "batchID", batchID, "rowCount", rowCount)
	return rowCount, nil
}

// RegisterStreamingWorkflow registers the streaming workflow and activities
func RegisterStreamingWorkflow(w worker.Worker) {
	w.RegisterWorkflow(StreamingWorkflow)
	w.RegisterActivity(GenerateBatchActivity)
	w.RegisterActivity(ProcessBatchActivity)
	w.RegisterActivity(ProcessBatchChunkActivity)
	w.RegisterActivity(StoreBatchActivity)
}
