package flight

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FlightClient is a client for the Arrow Flight server
type FlightClient struct {
	client    flight.Client
	addr      string
	allocator memory.Allocator
	conn      *grpc.ClientConn
}

// FlightClientConfig contains configuration options for the Flight client
type FlightClientConfig struct {
	// Address to connect to (e.g., "localhost:8080")
	Addr string
	// Memory allocator to use
	Allocator memory.Allocator
}

// NewFlightClient creates a new Arrow Flight client
func NewFlightClient(config FlightClientConfig) (*FlightClient, error) {
	if config.Addr == "" {
		config.Addr = "localhost:8080"
	}
	if config.Allocator == nil {
		config.Allocator = memory.NewGoAllocator()
	}

	// Set up gRPC options
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Set maximum message sizes for large batches
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024), // 64MB
			grpc.MaxCallSendMsgSize(64*1024*1024), // 64MB
		),
	}

	// Create a Flight client with the gRPC options
	client, err := flight.NewClientWithMiddleware(config.Addr, nil, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight client: %w", err)
	}

	return &FlightClient{
		client:    client,
		addr:      config.Addr,
		allocator: config.Allocator,
		conn:      nil, // We don't need to store the connection separately
	}, nil
}

// Close closes the Flight client
func (c *FlightClient) Close() error {
	c.client.Close()
	return nil
}

// PutBatch sends a batch to the Flight server and returns the batch ID
func (c *FlightClient) PutBatch(ctx context.Context, batch arrow.Record) (string, error) {
	// Create a Flight descriptor
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("put"),
	}

	// Start a DoPut stream
	stream, err := c.client.DoPut(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to start DoPut stream: %w", err)
	}

	// First, send the descriptor
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
	}); err != nil {
		return "", fmt.Errorf("failed to send descriptor: %w", err)
	}

	// Create a writer for the stream
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(batch.Schema()))

	// Write the batch to the stream
	if err := writer.Write(batch); err != nil {
		// Make sure to close the writer even if writing fails
		writer.Close()
		return "", fmt.Errorf("failed to write batch to stream: %w", err)
	}

	// Close the writer to signal the end of the stream
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	// Get the result
	result, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("failed to receive result: %w", err)
	}

	// Return the batch ID
	return string(result.AppMetadata), nil
}

// GetBatch retrieves a batch from the Flight server by ID
func (c *FlightClient) GetBatch(ctx context.Context, batchID string) (arrow.Record, error) {
	// Create a Flight ticket
	ticket := &flight.Ticket{
		Ticket: []byte(batchID),
	}

	// Start a DoGet stream
	stream, err := c.client.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("failed to start DoGet stream: %w", err)
	}

	// Create a reader for the stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Read the batch
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("error reading batch: %w", err)
		}
		return nil, fmt.Errorf("no batch received")
	}

	// Get the batch and retain it
	batch := reader.Record()
	batch.Retain() // Important: Retain the batch so it's not released when the reader is released

	return batch, nil
}

// ListBatches lists all batches in the Flight server
func (c *FlightClient) ListBatches(ctx context.Context) ([]string, error) {
	// Create a Flight criteria
	criteria := &flight.Criteria{}

	// Start a ListFlights stream
	stream, err := c.client.ListFlights(ctx, criteria)
	if err != nil {
		return nil, fmt.Errorf("failed to start ListFlights stream: %w", err)
	}

	// Read all flight infos
	var batchIDs []string
	for {
		info, err := stream.Recv()
		if err != nil {
			// Check if we've reached the end of the stream
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("error receiving flight info: %w", err)
		}
		batchIDs = append(batchIDs, string(info.FlightDescriptor.Cmd))
	}

	return batchIDs, nil
}
