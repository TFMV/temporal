package flight

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// startTestServer starts a Flight server for testing
func startTestServer(t *testing.T) (*FlightServer, string) {
	// Find an available port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err, "Failed to find available port")

	addr := listener.Addr().String()
	listener.Close() // Close the listener so the server can use the port

	// Create a server with a shorter TTL for testing
	server, err := NewFlightServer(FlightServerConfig{
		Addr:      addr,
		Allocator: memory.NewGoAllocator(),
		TTL:       5 * time.Minute, // Shorter TTL for testing
	})
	require.NoError(t, err, "Failed to create Flight server")

	// Start the server in a goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			// Only send error if it's not a "server closed" error
			if err != grpc.ErrServerStopped {
				serverErrCh <- err
			}
		}
	}()

	// Wait for the server to start or error
	select {
	case err := <-serverErrCh:
		t.Fatalf("Server failed to start: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Server started successfully
	}

	return server, addr
}

// createTestBatch creates a test batch with sample data
func createTestBatch(t *testing.T, allocator memory.Allocator) arrow.Record {
	// Create a schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create builders
	idBuilder := array.NewInt32Builder(allocator)
	defer idBuilder.Release()

	nameBuilder := array.NewStringBuilder(allocator)
	defer nameBuilder.Release()

	valueBuilder := array.NewFloat64Builder(allocator)
	defer valueBuilder.Release()

	// Add data
	idBuilder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	nameBuilder.AppendValues([]string{"one", "two", "three", "four", "five"}, nil)
	valueBuilder.AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)

	// Create arrays
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	valueArray := valueBuilder.NewArray()
	defer valueArray.Release()

	// Create record
	columns := []arrow.Array{idArray, nameArray, valueArray}
	batch := array.NewRecord(schema, columns, 5)

	return batch
}

// TestFlightServerClient tests the Flight server and client together
func TestFlightServerClient(t *testing.T) {
	// Start a server
	server, addr := startTestServer(t)
	defer server.Stop()

	// Create a client
	client, err := NewFlightClient(FlightClientConfig{
		Addr:      addr,
		Allocator: memory.NewGoAllocator(),
	})
	require.NoError(t, err, "Failed to create Flight client")
	defer client.Close()

	// Create a test batch
	batch := createTestBatch(t, memory.NewGoAllocator())
	defer batch.Release()

	// Test PutBatch with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batchID, err := client.PutBatch(ctx, batch)
	require.NoError(t, err, "Failed to put batch")
	require.NotEmpty(t, batchID, "Batch ID should not be empty")

	// Test GetBatch with timeout
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retrievedBatch, err := client.GetBatch(ctx, batchID)
	require.NoError(t, err, "Failed to get batch")
	defer retrievedBatch.Release()

	// Verify the batch
	assert.Equal(t, batch.NumRows(), retrievedBatch.NumRows(), "Number of rows should match")
	assert.Equal(t, batch.NumCols(), retrievedBatch.NumCols(), "Number of columns should match")

	// Test ListBatches with timeout
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batchIDs, err := client.ListBatches(ctx)
	require.NoError(t, err, "Failed to list batches")
	assert.Contains(t, batchIDs, batchID, "Batch ID should be in the list")
}

// TestFlightServerClientLargeBatch tests the Flight server and client with a large batch
func TestFlightServerClientLargeBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large batch test in short mode")
	}

	// Start a server
	server, addr := startTestServer(t)
	defer server.Stop()

	// Create a client
	client, err := NewFlightClient(FlightClientConfig{
		Addr:      addr,
		Allocator: memory.NewGoAllocator(),
	})
	require.NoError(t, err, "Failed to create Flight client")
	defer client.Close()

	// Create a large test batch (100,000 rows)
	allocator := memory.NewGoAllocator()

	// Create a schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create builders
	idBuilder := array.NewInt32Builder(allocator)
	defer idBuilder.Release()

	valueBuilder := array.NewFloat64Builder(allocator)
	defer valueBuilder.Release()

	// Add data
	numRows := 100000
	ids := make([]int32, numRows)
	values := make([]float64, numRows)

	for i := 0; i < numRows; i++ {
		ids[i] = int32(i)
		values[i] = float64(i) * 1.1
	}

	idBuilder.AppendValues(ids, nil)
	valueBuilder.AppendValues(values, nil)

	// Create arrays
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	valueArray := valueBuilder.NewArray()
	defer valueArray.Release()

	// Create record
	columns := []arrow.Array{idArray, valueArray}
	batch := array.NewRecord(schema, columns, int64(numRows))
	defer batch.Release()

	// Test PutBatch with a longer timeout for large batch
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	batchID, err := client.PutBatch(ctx, batch)
	elapsed := time.Since(start)
	require.NoError(t, err, "Failed to put large batch")
	require.NotEmpty(t, batchID, "Batch ID should not be empty")

	t.Logf("Put large batch (%d rows) in %s", numRows, elapsed)

	// Test GetBatch with a longer timeout for large batch
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start = time.Now()
	retrievedBatch, err := client.GetBatch(ctx, batchID)
	elapsed = time.Since(start)
	require.NoError(t, err, "Failed to get large batch")
	defer retrievedBatch.Release()

	t.Logf("Got large batch (%d rows) in %s", numRows, elapsed)

	// Verify the batch
	assert.Equal(t, batch.NumRows(), retrievedBatch.NumRows(), "Number of rows should match")
	assert.Equal(t, batch.NumCols(), retrievedBatch.NumCols(), "Number of columns should match")

	// Verify some values
	idCol := retrievedBatch.Column(0).(*array.Int32)
	valueCol := retrievedBatch.Column(1).(*array.Float64)

	// Check first, middle, and last values
	assert.Equal(t, int32(0), idCol.Value(0), "First ID should match")
	assert.Equal(t, float64(0), valueCol.Value(0), "First value should match")

	middle := numRows / 2
	assert.Equal(t, int32(middle), idCol.Value(middle), "Middle ID should match")
	assert.Equal(t, float64(middle)*1.1, valueCol.Value(middle), "Middle value should match")

	last := numRows - 1
	assert.Equal(t, int32(last), idCol.Value(last), "Last ID should match")
	assert.Equal(t, float64(last)*1.1, valueCol.Value(last), "Last value should match")
}
