package flight

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"google.golang.org/grpc"
)

// FlightServer implements a simple Arrow Flight server for sharing Arrow RecordBatches
// between Temporal activities with minimal serialization.
type FlightServer struct {
	flight.BaseFlightServer
	server      *grpc.Server
	listener    net.Listener
	addr        string
	batches     map[string]arrow.Record
	batchesMu   sync.RWMutex
	allocator   memory.Allocator
	expirations map[string]time.Time
	ttl         time.Duration
	cancel      context.CancelFunc // Cancel function for cleanup goroutine
}

// FlightServerConfig contains configuration options for the Flight server
type FlightServerConfig struct {
	// Address to listen on (e.g., "localhost:8080")
	Addr string
	// Memory allocator to use
	Allocator memory.Allocator
	// TTL for stored batches (default: 1 hour)
	TTL time.Duration
}

// NewFlightServer creates a new Arrow Flight server
func NewFlightServer(config FlightServerConfig) (*FlightServer, error) {
	if config.Addr == "" {
		config.Addr = "localhost:8080"
	}
	if config.Allocator == nil {
		config.Allocator = memory.NewGoAllocator()
	}
	if config.TTL == 0 {
		config.TTL = 1 * time.Hour
	}

	// Create the server without starting the listener yet
	server := &FlightServer{
		addr:        config.Addr,
		batches:     make(map[string]arrow.Record),
		expirations: make(map[string]time.Time),
		allocator:   config.Allocator,
		ttl:         config.TTL,
	}

	// Create a gRPC server with appropriate options
	server.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64MB max message size
		grpc.MaxSendMsgSize(64*1024*1024), // 64MB max message size
	)

	// Register the Flight service
	flight.RegisterFlightServiceServer(server.server, server)

	// Start a goroutine to clean up expired batches
	ctx, cancel := context.WithCancel(context.Background())
	server.cancel = cancel
	go server.cleanupExpiredBatches(ctx)

	return server, nil
}

// Start starts the Flight server
func (s *FlightServer) Start() error {
	fmt.Printf("Starting Arrow Flight server on %s\n", s.addr)

	// Create a listener
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	// Store the listener
	s.listener = listener

	// Serve in the current goroutine
	return s.server.Serve(listener)
}

// Stop stops the Flight server
func (s *FlightServer) Stop() {
	fmt.Println("Stopping Arrow Flight server")

	// Cancel the cleanup goroutine
	if s.cancel != nil {
		s.cancel()
	}

	// Clear all batches to release memory
	s.batchesMu.Lock()
	for id, batch := range s.batches {
		batch.Release()
		delete(s.batches, id)
		delete(s.expirations, id)
	}
	s.batchesMu.Unlock()

	// Stop the gRPC server gracefully
	if s.server != nil {
		s.server.GracefulStop()
	}

	// Close the listener if it exists
	if s.listener != nil {
		s.listener.Close()
	}

	fmt.Println("Arrow Flight server stopped")
}

// GetFlightInfo implements the Flight GetFlightInfo method
func (s *FlightServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	cmd := string(request.Cmd)

	s.batchesMu.RLock()
	batch, ok := s.batches[cmd]
	s.batchesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("batch with ID %s not found", cmd)
	}

	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{Ticket: []byte(cmd)},
		Location: []*flight.Location{
			{Uri: fmt.Sprintf("grpc://%s", s.addr)},
		},
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(batch.Schema(), s.allocator),
		FlightDescriptor: request,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     batch.NumRows(),
		TotalBytes:       -1, // Unknown size
	}, nil
}

// DoGet implements the Flight DoGet method
func (s *FlightServer) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	batchID := string(request.Ticket)

	s.batchesMu.RLock()
	batch, ok := s.batches[batchID]
	s.batchesMu.RUnlock()

	if !ok {
		return fmt.Errorf("batch with ID %s not found", batchID)
	}

	// Create a writer for the stream
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(batch.Schema()))

	// Write the batch to the stream
	if err := writer.Write(batch); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write batch to stream: %w", err)
	}

	// Close the writer to signal the end of the stream
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

// DoPut implements the Flight DoPut method
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// Get the first message which should contain the descriptor
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive descriptor: %w", err)
	}

	// Check if we have a descriptor
	if firstMsg.FlightDescriptor == nil {
		return fmt.Errorf("missing flight descriptor in first message")
	}

	// Create a reader for the stream
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Read all records (should be just one in our case)
	var batch arrow.Record
	for reader.Next() {
		if batch != nil {
			batch.Release()
		}
		batch = reader.Record()
		batch.Retain() // Retain the batch so it's not released when the reader is released
	}

	if err := reader.Err(); err != nil {
		if batch != nil {
			batch.Release()
		}
		return fmt.Errorf("error reading record: %w", err)
	}

	if batch == nil {
		return fmt.Errorf("no record received")
	}

	// Generate a unique ID for the batch
	batchID := generateBatchID()

	// Store the batch
	s.batchesMu.Lock()
	s.batches[batchID] = batch
	s.expirations[batchID] = time.Now().Add(s.ttl)
	s.batchesMu.Unlock()

	// Send the batch ID back to the client
	err = stream.Send(&flight.PutResult{
		AppMetadata: []byte(batchID),
	})
	if err != nil {
		// If we fail to send the result, remove the batch from storage
		s.batchesMu.Lock()
		if storedBatch, ok := s.batches[batchID]; ok {
			storedBatch.Release()
			delete(s.batches, batchID)
			delete(s.expirations, batchID)
		}
		s.batchesMu.Unlock()
		return fmt.Errorf("failed to send result: %w", err)
	}

	return nil
}

// ListFlights implements the Flight ListFlights method
func (s *FlightServer) ListFlights(request *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	s.batchesMu.RLock()
	defer s.batchesMu.RUnlock()

	for batchID, batch := range s.batches {
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(batchID),
		}

		endpoint := &flight.FlightEndpoint{
			Ticket: &flight.Ticket{Ticket: []byte(batchID)},
			Location: []*flight.Location{
				{Uri: fmt.Sprintf("grpc://%s", s.addr)},
			},
		}

		info := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(batch.Schema(), s.allocator),
			FlightDescriptor: descriptor,
			Endpoint:         []*flight.FlightEndpoint{endpoint},
			TotalRecords:     batch.NumRows(),
			TotalBytes:       -1, // Unknown size
		}

		if err := stream.Send(info); err != nil {
			return err
		}
	}

	return nil
}

// cleanupExpiredBatches periodically removes expired batches
func (s *FlightServer) cleanupExpiredBatches(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.performCleanup()
		case <-ctx.Done():
			fmt.Println("Stopping batch cleanup routine")
			return
		}
	}
}

// performCleanup handles the actual cleanup of expired batches
func (s *FlightServer) performCleanup() {
	now := time.Now()
	var expiredIDs []string

	// Find expired batches
	s.batchesMu.RLock()
	for batchID, expiration := range s.expirations {
		if now.After(expiration) {
			expiredIDs = append(expiredIDs, batchID)
		}
	}
	s.batchesMu.RUnlock()

	// Remove expired batches
	if len(expiredIDs) > 0 {
		s.batchesMu.Lock()
		for _, batchID := range expiredIDs {
			if batch, ok := s.batches[batchID]; ok {
				batch.Release()
				delete(s.batches, batchID)
				delete(s.expirations, batchID)
			}
		}
		s.batchesMu.Unlock()
		fmt.Printf("Cleaned up %d expired batches\n", len(expiredIDs))
	}
}

// StoreBatch stores a batch in the server and returns a unique ID
func (s *FlightServer) StoreBatch(batch arrow.Record) string {
	batchID := generateBatchID()

	// Retain the batch so it's not released when the caller releases it
	batch.Retain()

	s.batchesMu.Lock()
	s.batches[batchID] = batch
	s.expirations[batchID] = time.Now().Add(s.ttl)
	s.batchesMu.Unlock()

	return batchID
}

// RetrieveBatch retrieves a batch from the server by ID
func (s *FlightServer) RetrieveBatch(batchID string) (arrow.Record, error) {
	s.batchesMu.RLock()
	batch, ok := s.batches[batchID]
	s.batchesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("batch with ID %s not found", batchID)
	}

	// Update the expiration time
	s.batchesMu.Lock()
	s.expirations[batchID] = time.Now().Add(s.ttl)
	s.batchesMu.Unlock()

	// Retain the batch so it's not released when we remove it from the map
	batch.Retain()
	return batch, nil
}

// ReleaseBatch releases a batch from the server
func (s *FlightServer) ReleaseBatch(batchID string) {
	s.batchesMu.Lock()
	defer s.batchesMu.Unlock()

	if batch, ok := s.batches[batchID]; ok {
		batch.Release()
		delete(s.batches, batchID)
		delete(s.expirations, batchID)
	}
}

// generateBatchID generates a unique batch ID
func generateBatchID() string {
	return fmt.Sprintf("batch-%d", time.Now().UnixNano())
}

// Serve starts the Flight server
func (s *FlightServer) Serve() error {
	// Create a listener
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	// Create a gRPC server
	s.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024), // 1GB
		grpc.MaxSendMsgSize(1024*1024*1024), // 1GB
	)

	// Register the Flight service
	flight.RegisterFlightServiceServer(s.server, s)

	// Start the server
	return s.server.Serve(lis)
}

// Shutdown stops the Flight server
func (s *FlightServer) Shutdown() error {
	if s.server != nil {
		s.server.GracefulStop()
	}
	return nil
}
