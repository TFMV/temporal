package arrow

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	// MetadataEncodingType is the metadata key for the encoding type
	MetadataEncodingType = "encoding-type"

	// EncodingTypeArrowRecord is the encoding type for Arrow Record
	EncodingTypeArrowRecord = "arrow/record"
)

// ArrowDataConverter is a custom Temporal DataConverter that handles Arrow data types
// with zero-copy operations where possible
type ArrowDataConverter struct {
	pool          memory.Allocator
	baseConverter converter.DataConverter
}

// NewArrowDataConverter creates a new ArrowDataConverter with the specified memory allocator
func NewArrowDataConverter(pool memory.Allocator) *ArrowDataConverter {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	return &ArrowDataConverter{
		pool:          pool,
		baseConverter: converter.GetDefaultDataConverter(),
	}
}

// ToPayload converts a value to a Temporal payload
// It handles Arrow Record objects specially for efficient serialization
func (c *ArrowDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	// Handle Arrow Record objects
	if record, ok := value.(arrow.Record); ok {
		return c.recordToPayload(record)
	}

	// Fall back to default converter for other types
	return c.baseConverter.ToPayload(value)
}

// FromPayload converts a Temporal payload to a value
// It handles Arrow Record objects specially for efficient deserialization
func (c *ArrowDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	// Check if this is an Arrow Record payload
	if encodingType, ok := payload.Metadata[MetadataEncodingType]; ok && string(encodingType) == EncodingTypeArrowRecord {
		// Handle Arrow Record deserialization
		return c.payloadToRecord(payload, valuePtr)
	}

	// Fall back to default converter for other types
	return c.baseConverter.FromPayload(payload, valuePtr)
}

// ToPayloads converts multiple values to Temporal payloads
func (c *ArrowDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	if len(values) == 0 {
		return &commonpb.Payloads{}, nil
	}

	result := &commonpb.Payloads{
		Payloads: make([]*commonpb.Payload, len(values)),
	}

	for i, value := range values {
		payload, err := c.ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value at index %d: %w", i, err)
		}
		result.Payloads[i] = payload
	}

	return result, nil
}

// FromPayloads converts Temporal payloads to values
func (c *ArrowDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if len(payloads.GetPayloads()) != len(valuePtrs) {
		return fmt.Errorf("number of payloads (%d) does not match number of values (%d)",
			len(payloads.GetPayloads()), len(valuePtrs))
	}

	for i, payload := range payloads.GetPayloads() {
		err := c.FromPayload(payload, valuePtrs[i])
		if err != nil {
			return fmt.Errorf("failed to convert payload at index %d: %w", i, err)
		}
	}

	return nil
}

// recordToPayload converts an Arrow Record to a Temporal payload
// This is optimized for minimal copying
func (c *ArrowDataConverter) recordToPayload(record arrow.Record) (*commonpb.Payload, error) {
	var buf bytes.Buffer

	// Create an Arrow IPC writer with the record's schema
	writer := ipc.NewWriter(&buf,
		ipc.WithSchema(record.Schema()),
		ipc.WithAllocator(c.pool),
		ipc.WithDictionaryDeltas(true))
	defer writer.Close()

	// Write the record
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to serialize Arrow record: %w", err)
	}

	// Create the payload with Arrow-specific metadata
	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			MetadataEncodingType: []byte(EncodingTypeArrowRecord),
		},
		Data: buf.Bytes(),
	}

	return payload, nil
}

// payloadToRecord converts a Temporal payload to an Arrow Record
// This is optimized for zero-copy operations where possible
func (c *ArrowDataConverter) payloadToRecord(payload *commonpb.Payload, valuePtr interface{}) error {
	// Check that the target is a pointer to an arrow.Record
	recordPtr, ok := valuePtr.(*arrow.Record)
	if !ok {
		return fmt.Errorf("target value is not a *arrow.Record")
	}

	// Create a reader for the payload data
	reader := bytes.NewReader(payload.Data)

	// Create an Arrow IPC reader
	ipcReader, err := ipc.NewReader(reader,
		ipc.WithAllocator(c.pool),
		ipc.WithEnsureNativeEndian(true))
	if err != nil {
		return fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer ipcReader.Release()

	// Read the record
	if !ipcReader.Next() {
		return fmt.Errorf("no record found in payload")
	}

	// Get the record and retain it
	record := ipcReader.Record()
	record.Retain() // Important: Ensure the record isn't released when the reader is closed

	// Set the output value
	*recordPtr = record

	return nil
}

// GetDefaultDataConverter returns the default data converter
func (c *ArrowDataConverter) GetDefaultDataConverter() converter.DataConverter {
	return c.baseConverter
}

// BatchProcessor is an interface for processing Arrow RecordBatches
// This enables streaming processing of data
type BatchProcessor interface {
	// ProcessBatch processes a single RecordBatch and returns a result
	ProcessBatch(batch arrow.Record, config map[string]string) (arrow.Record, error)

	// Release releases any resources held by the processor
	Release()
}

// StreamingBatchIterator provides an iterator interface for streaming Arrow RecordBatches
type StreamingBatchIterator interface {
	// Next returns the next batch in the stream
	// Returns nil when there are no more batches
	Next() (arrow.Record, error)

	// Release releases any resources held by the iterator
	Release()
}

// BatchWriter is an interface for writing Arrow RecordBatches
type BatchWriter interface {
	// WriteBatch writes a single RecordBatch
	WriteBatch(batch arrow.Record) error

	// Close closes the writer and releases any resources
	Close() error
}
