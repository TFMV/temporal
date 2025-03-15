package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// Default activity timeouts for Flight workflow
	flightStartToCloseTimeout    = 10 * time.Minute
	flightScheduleToStartTimeout = 1 * time.Minute
	flightHeartbeatTimeout       = 30 * time.Second

	// Default retry policy for Flight workflow
	flightMaxAttempts        = 3
	flightInitialInterval    = 1 * time.Second
	flightMaximumInterval    = 1 * time.Minute
	flightBackoffCoefficient = 2.0

	// TODO: Default batch processing parameters for Flight workflow
	flightMaxBatchSize = 50000 // Maximum rows per batch to avoid gRPC message size limits
)

// FlightWorkflowParams contains all parameters for the Flight workflow
type FlightWorkflowParams struct {
	BatchSize  int     `json:"batchSize"`
	NumBatches int     `json:"numBatches"`
	Threshold  float64 `json:"threshold"`
	// Flight server configuration
	FlightServerAddr string `json:"flightServerAddr"`
}

// FlightWorkflow demonstrates processing Arrow data using Arrow Flight for efficient data transfer
// This workflow uses the Flight server to transfer data between activities with minimal serialization
func FlightWorkflow(ctx workflow.Context, params FlightWorkflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Arrow Flight workflow",
		"batchSize", params.BatchSize,
		"numBatches", params.NumBatches,
		"threshold", params.Threshold,
		"flightServerAddr", params.FlightServerAddr)

	// Activity options for data processing activities with optimized settings for gRPC
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout:    flightStartToCloseTimeout,
		ScheduleToStartTimeout: flightScheduleToStartTimeout,
		HeartbeatTimeout:       flightHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    flightInitialInterval,
			MaximumInterval:    flightMaximumInterval,
			BackoffCoefficient: flightBackoffCoefficient,
			MaximumAttempts:    flightMaxAttempts,
			NonRetryableErrorTypes: []string{
				"InvalidArgument",
				"SchemaValidationError",
			},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Create Flight configuration
	flightConfig := FlightConfig{
		ServerAddr:       params.FlightServerAddr,
		StartLocalServer: true, // Start a local server if one is not available
	}

	// Process batches in a streaming fashion
	var processedCount int64
	for i := 0; i < params.NumBatches; i++ {
		// Generate a batch of data and store it in the Flight server
		var batchID string
		err := workflow.ExecuteActivity(ctx, FlightGenerateBatchActivity, params.BatchSize, flightConfig).Get(ctx, &batchID)
		if err != nil {
			return fmt.Errorf("failed to generate batch %d: %w", i, err)
		}

		// Process the batch using the Flight server
		var processedBatchID string
		err = workflow.ExecuteActivity(ctx, FlightProcessBatchActivity, batchID, params.Threshold, flightConfig).Get(ctx, &processedBatchID)
		if err != nil {
			return fmt.Errorf("failed to process batch %d: %w", i, err)
		}

		// Store the processed batch
		var storedCount int64
		err = workflow.ExecuteActivity(ctx, FlightStoreBatchActivity, processedBatchID, flightConfig).Get(ctx, &storedCount)
		if err != nil {
			return fmt.Errorf("failed to store batch %d: %w", i, err)
		}

		processedCount += storedCount

		// Log progress
		logger.Info("Processed batch", "batchNumber", i, "rowsProcessed", storedCount)
	}

	logger.Info("Completed Flight workflow", "totalRowsProcessed", processedCount)
	return nil
}

// RegisterFlightWorkflow registers the Flight workflow and activities with the worker
func RegisterFlightWorkflow(w worker.Worker) {
	// Register the workflow
	w.RegisterWorkflow(FlightWorkflow)

	// Register the activities
	RegisterFlightActivities(w)
}
