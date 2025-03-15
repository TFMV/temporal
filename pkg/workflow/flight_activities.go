package workflow

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	arrow_utils "github.com/TFMV/temporal/pkg/arrow"
	"github.com/TFMV/temporal/pkg/flight"
)

// FlightConfig contains configuration for the Arrow Flight server
type FlightConfig struct {
	// Address of the Flight server (e.g., "localhost:8080")
	ServerAddr string
	// Whether to start a local server if one is not available
	StartLocalServer bool
	// Memory allocator to use
	Allocator memory.Allocator
}

// FlightContext contains context for Arrow Flight operations
type FlightContext struct {
	// Flight client
	Client *flight.FlightClient
	// Flight server (if started locally)
	Server *flight.FlightServer
	// Whether the server was started locally
	LocalServer bool
}

// GetFlightContext gets or creates a Flight context
func GetFlightContext(ctx context.Context, config FlightConfig) (*FlightContext, error) {
	// Use default allocator if not provided
	if config.Allocator == nil {
		config.Allocator = memory.NewGoAllocator()
	}

	// Use default address if not provided
	if config.ServerAddr == "" {
		config.ServerAddr = "localhost:8080"
	}

	// Try to connect to the server
	client, err := flight.NewFlightClient(flight.FlightClientConfig{
		Addr:      config.ServerAddr,
		Allocator: config.Allocator,
	})

	// If connection failed and we're allowed to start a local server
	if err != nil && config.StartLocalServer {
		// Start a local server
		server, err := flight.NewFlightServer(flight.FlightServerConfig{
			Addr:      config.ServerAddr,
			Allocator: config.Allocator,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Flight server: %w", err)
		}

		// Start the server in a goroutine
		go func() {
			if err := server.Start(); err != nil {
				log.Printf("Flight server error: %v", err)
			}
		}()

		// Wait for the server to start
		time.Sleep(100 * time.Millisecond)

		// Try to connect again
		client, err = flight.NewFlightClient(flight.FlightClientConfig{
			Addr:      config.ServerAddr,
			Allocator: config.Allocator,
		})
		if err != nil {
			server.Stop()
			return nil, fmt.Errorf("failed to connect to local Flight server: %w", err)
		}

		return &FlightContext{
			Client:      client,
			Server:      server,
			LocalServer: true,
		}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Flight server at %s: %w", config.ServerAddr, err)
	}

	return &FlightContext{
		Client:      client,
		LocalServer: false,
	}, nil
}

// CloseFlightContext closes a Flight context
func CloseFlightContext(ctx *FlightContext) error {
	if ctx == nil {
		return nil
	}

	var clientErr, serverErr error

	// Close the client
	if ctx.Client != nil {
		clientErr = ctx.Client.Close()
	}

	// Stop the server if it was started locally
	if ctx.LocalServer && ctx.Server != nil {
		ctx.Server.Stop()
	}

	if clientErr != nil {
		return clientErr
	}
	return serverErr
}

// FlightGenerateBatchActivity generates a batch and stores it in the Flight server
func FlightGenerateBatchActivity(ctx context.Context, batchSize int, flightConfig FlightConfig) (string, error) {
	// Get activity info for logging
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	logger.Info("Starting FlightGenerateBatchActivity", "ActivityID", info.ActivityID)

	// Record heartbeats
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()

	go func() {
		for range heartbeat.C {
			activity.RecordHeartbeat(ctx, "Generating batch")
		}
	}()

	// Get Flight context
	flightCtx, err := GetFlightContext(ctx, flightConfig)
	if err != nil {
		return "", fmt.Errorf("failed to get Flight context: %w", err)
	}
	defer func() {
		if err := CloseFlightContext(flightCtx); err != nil {
			logger.Error("Failed to close flight context", "error", err)
		}
	}()

	// Generate a batch
	batch, err := generateArrowBatch(batchSize)
	if err != nil {
		return "", fmt.Errorf("failed to generate batch: %w", err)
	}
	defer batch.Release()

	// Store the batch in the Flight server
	batchID, err := flightCtx.Client.PutBatch(ctx, batch)
	if err != nil {
		return "", fmt.Errorf("failed to store batch in Flight server: %w", err)
	}

	logger.Info("Generated batch", "BatchID", batchID, "NumRows", batch.NumRows())
	return batchID, nil
}

// generateArrowBatch creates a new Arrow batch with random data
func generateArrowBatch(batchSize int) (arrow.Record, error) {
	// This is a simplified version - in a real implementation, you would create a more complex batch
	return arrow_utils.GenerateRandomBatch(batchSize)
}

// FlightProcessBatchActivity processes a batch from the Flight server
func FlightProcessBatchActivity(ctx context.Context, batchID string, threshold float64, flightConfig FlightConfig) (string, error) {
	// Get activity info for logging
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	logger.Info("Starting FlightProcessBatchActivity", "ActivityID", info.ActivityID, "BatchID", batchID)

	// Record heartbeats
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()

	go func() {
		for range heartbeat.C {
			activity.RecordHeartbeat(ctx, "Processing batch")
		}
	}()

	// Get Flight context
	flightCtx, err := GetFlightContext(ctx, flightConfig)
	if err != nil {
		return "", fmt.Errorf("failed to get Flight context: %w", err)
	}
	defer func() {
		if err := CloseFlightContext(flightCtx); err != nil {
			logger.Error("Failed to close flight context", "error", err)
		}
	}()

	// Retrieve the batch from the Flight server
	batch, err := flightCtx.Client.GetBatch(ctx, batchID)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve batch from Flight server: %w", err)
	}
	defer batch.Release()

	// Process the batch
	processedBatch, err := processArrowBatch(batch, threshold)
	if err != nil {
		return "", fmt.Errorf("failed to process batch: %w", err)
	}
	defer processedBatch.Release()

	// Store the processed batch in the Flight server
	processedBatchID, err := flightCtx.Client.PutBatch(ctx, processedBatch)
	if err != nil {
		return "", fmt.Errorf("failed to store processed batch in Flight server: %w", err)
	}

	logger.Info("Processed batch", "BatchID", batchID, "ProcessedBatchID", processedBatchID, "NumRows", processedBatch.NumRows())
	return processedBatchID, nil
}

// processArrowBatch processes an Arrow batch
func processArrowBatch(batch arrow.Record, threshold float64) (arrow.Record, error) {
	// This is a simplified version - in a real implementation, you would do more complex processing
	return arrow_utils.FilterBatch(batch, threshold)
}

// FlightStoreBatchActivity stores a batch from the Flight server
func FlightStoreBatchActivity(ctx context.Context, batchID string, flightConfig FlightConfig) (int64, error) {
	// Get activity info for logging
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	logger.Info("Starting FlightStoreBatchActivity", "ActivityID", info.ActivityID, "BatchID", batchID)

	// Record heartbeats
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()

	go func() {
		for range heartbeat.C {
			activity.RecordHeartbeat(ctx, "Storing batch")
		}
	}()

	// Get Flight context
	flightCtx, err := GetFlightContext(ctx, flightConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to get Flight context: %w", err)
	}
	defer func() {
		if err := CloseFlightContext(flightCtx); err != nil {
			logger.Error("Failed to close flight context", "error", err)
		}
	}()

	// Retrieve the batch from the Flight server
	batch, err := flightCtx.Client.GetBatch(ctx, batchID)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve batch from Flight server: %w", err)
	}
	defer batch.Release()

	// Store the batch (in this example, we just return the number of rows)
	numRows := batch.NumRows()

	logger.Info("Stored batch", "BatchID", batchID, "NumRows", numRows)
	return numRows, nil
}

// RegisterFlightActivities registers the Flight activities with the worker
func RegisterFlightActivities(w worker.Worker) {
	w.RegisterActivity(FlightGenerateBatchActivity)
	w.RegisterActivity(FlightProcessBatchActivity)
	w.RegisterActivity(FlightStoreBatchActivity)
}
