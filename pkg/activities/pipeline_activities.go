package activities

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/TFMV/temporal/pkg/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
)

// PipelineActivities holds the activities for the data pipeline
type PipelineActivities struct {
	serializer *arrow.Serializer
	allocator  memory.Allocator
	logger     log.Logger
}

// NewPipelineActivities creates a new instance of PipelineActivities
func NewPipelineActivities(allocator memory.Allocator) *PipelineActivities {
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}

	// Create a standard Go logger and wrap it with slog
	stdLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	return &PipelineActivities{
		serializer: arrow.NewSerializer(allocator),
		allocator:  allocator,
		logger:     log.NewStructuredLogger(stdLogger),
	}
}

// IngestDataActivity ingests data and returns it as serialized Arrow data
// This is the first stage of the pipeline
func (a *PipelineActivities) IngestDataActivity(ctx context.Context, sourceConfig map[string]string, batchSize int) ([]byte, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting data ingestion", "batchSize", batchSize)

	// In a real implementation, this would connect to Kafka, gRPC, or another source
	// For this example, we'll create sample data

	// Create a sample Arrow table
	// This creates columnar data in memory, optimized for processing
	table := arrow.CreateSampleTable(batchSize, a.allocator)
	defer table.Release() // Ensure resources are properly released

	logger.Info("Created sample data table",
		"rows", table.NumRows(),
		"columns", table.NumCols())

	// Serialize the table for passing to the next activity
	// This preserves the columnar format and enables zero-copy deserialization
	serializedData, err := a.serializer.SerializeTable(table)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize Arrow table: %w", err)
	}

	logger.Info("Data ingestion completed", "dataSize", len(serializedData))
	return serializedData, nil
}

// ProcessDataActivity processes the Arrow data while maintaining columnar format
// This is the second stage of the pipeline
func (a *PipelineActivities) ProcessDataActivity(ctx context.Context, serializedData []byte, processingConfig map[string]string) ([]byte, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting data processing", "dataSize", len(serializedData))

	// Deserialize the Arrow table using zero-copy operations where possible
	table, err := a.serializer.DeserializeTable(serializedData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Arrow table: %w", err)
	}
	defer table.Release() // Ensure resources are properly released

	logger.Info("Deserialized table for processing",
		"rows", table.NumRows(),
		"columns", table.NumCols())

	// Extract filter threshold from processing config
	threshold := 5.0 // Default value
	if thresholdStr, ok := processingConfig["filterThreshold"]; ok {
		if parsedThreshold, err := strconv.ParseFloat(thresholdStr, 64); err == nil {
			threshold = parsedThreshold
		}
	}

	// Process the data - in this example, we'll filter rows using vectorized operations
	// This is an example of efficient columnar processing
	filteredTable, err := arrow.FilterTable(table, threshold, a.allocator)
	if err != nil {
		return nil, fmt.Errorf("failed to filter table: %w", err)
	}
	defer filteredTable.Release() // Ensure resources are properly released

	logger.Info("Processed data",
		"originalRows", table.NumRows(),
		"filteredRows", filteredTable.NumRows(),
		"threshold", threshold)

	// Serialize the processed table for passing to the next activity
	// Again, using Arrow's efficient IPC format
	processedData, err := a.serializer.SerializeTable(filteredTable)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize processed table: %w", err)
	}

	logger.Info("Data processing completed", "processedDataSize", len(processedData))
	return processedData, nil
}

// StoreDataActivity stores the processed Arrow data
// This is the final stage of the pipeline
func (a *PipelineActivities) StoreDataActivity(ctx context.Context, serializedData []byte, sinkConfig map[string]string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting data storage", "dataSize", len(serializedData))

	// Deserialize the Arrow table using zero-copy operations where possible
	table, err := a.serializer.DeserializeTable(serializedData)
	if err != nil {
		return fmt.Errorf("failed to deserialize Arrow table: %w", err)
	}
	defer table.Release() // Ensure resources are properly released

	logger.Info("Deserialized table for storage",
		"rows", table.NumRows(),
		"columns", table.NumCols())

	// In a real implementation, this would store the data in an object store or send via gRPC
	// For this example, we'll calculate and log statistics

	// Calculate some statistics on the data to simulate processing
	var totalValue float64
	var count int64

	// Process each column's data
	// We need to extract the typed arrays from the column chunks
	for i := 0; i < int(table.NumCols()); i++ {
		// Skip if not the value column (index 1)
		if i != 1 {
			continue
		}

		// Get the column's chunked data
		valueChunked := table.Column(i).Data()

		// Process each chunk in the column
		for _, chunk := range valueChunked.Chunks() {
			// Type assert to Float64 array
			if valueArray, ok := chunk.(*array.Float64); ok {
				// Process each value in the chunk
				for j := 0; j < valueArray.Len(); j++ {
					if !valueArray.IsNull(j) {
						totalValue += valueArray.Value(j)
						count++
					}
				}
			}
		}
	}

	avgValue := 0.0
	if count > 0 {
		avgValue = totalValue / float64(count)
	}

	logger.Info("Data statistics",
		"count", count,
		"totalValue", totalValue,
		"avgValue", avgValue)

	// In a real implementation, we would:
	// 1. Serialize the data in the appropriate format (e.g., Parquet, CSV)
	// 2. Upload to object storage (S3, GCS, etc.) or send via gRPC
	// 3. Return metadata about the stored data

	logger.Info("Data storage completed successfully")
	return nil
}
