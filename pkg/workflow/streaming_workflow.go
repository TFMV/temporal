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

// StreamingWorkflow demonstrates processing Arrow data in a streaming fashion
// using RecordBatches directly to minimize memory usage and maximize performance
func StreamingWorkflow(ctx workflow.Context, batchSize int, numBatches int, threshold float64) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting streaming Arrow workflow", "batchSize", batchSize, "numBatches", numBatches)

	// Activity options for data processing activities
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Process batches in a streaming fashion
	var processedCount int
	for i := 0; i < numBatches; i++ {
		// Generate a batch of data
		var batch arrowpkg.Record
		err := workflow.ExecuteActivity(ctx, GenerateBatchActivity, batchSize, i*batchSize).Get(ctx, &batch)
		if err != nil {
			return fmt.Errorf("failed to generate batch %d: %w", i, err)
		}

		// Process the batch
		var processedBatch arrowpkg.Record
		err = workflow.ExecuteActivity(ctx, ProcessBatchActivity, batch, threshold).Get(ctx, &processedBatch)
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

	logger.Info("Completed streaming workflow", "totalRowsProcessed", processedCount)
	return nil
}

// GenerateBatchActivity generates a sample Arrow RecordBatch
func GenerateBatchActivity(ctx context.Context, batchSize int, offset int) (arrowpkg.Record, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Generating batch", "batchSize", batchSize, "offset", offset)

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

// StoreBatchActivity stores an Arrow RecordBatch
func StoreBatchActivity(ctx context.Context, batch arrowpkg.Record, batchID string) (int, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Storing batch", "batchID", batchID, "numRows", batch.NumRows())

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
	w.RegisterActivity(StoreBatchActivity)
}
