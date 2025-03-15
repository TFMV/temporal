package arrow

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

// Serializer handles efficient serialization/deserialization of Arrow data
// with a focus on zero-copy operations
type Serializer struct {
	pool memory.Allocator
}

// NewSerializer creates a new Arrow Serializer with the specified memory allocator
func NewSerializer(pool memory.Allocator) *Serializer {
	// If no allocator is provided, use the default Go allocator
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	return &Serializer{pool: pool}
}

// SerializeRecord converts an Arrow Record to bytes using IPC format
// This preserves the columnar format and enables zero-copy deserialization
func (s *Serializer) SerializeRecord(record arrow.Record) ([]byte, error) {
	var buf bytes.Buffer

	// Create an Arrow IPC writer with the record's schema
	writer := ipc.NewWriter(&buf,
		ipc.WithSchema(record.Schema()),
		ipc.WithAllocator(s.pool),
		ipc.WithDictionaryDeltas(true))
	defer writer.Close()

	// Write the record
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to serialize record batch: %w", err)
	}

	return buf.Bytes(), nil
}

// DeserializeRecord converts bytes back to an Arrow Record
// Zero-copy operations are applied where possible during deserialization
func (s *Serializer) DeserializeRecord(data []byte) (arrow.Record, error) {
	reader := bytes.NewReader(data)

	// Create an Arrow IPC reader with the specified allocator
	ipcReader, err := ipc.NewReader(reader,
		ipc.WithAllocator(s.pool),
		ipc.WithEnsureNativeEndian(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer ipcReader.Release()

	// Read the record
	if !ipcReader.Next() {
		return nil, fmt.Errorf("no record found in data")
	}

	// Get the record and retain it
	record := ipcReader.Record()
	record.Retain() // Important: Ensure the record isn't released when the reader is closed

	return record, nil
}

// SerializeTable converts an Arrow Table to bytes using IPC format
// This preserves the columnar format and enables zero-copy deserialization
func (s *Serializer) SerializeTable(table arrow.Table) ([]byte, error) {
	var buf bytes.Buffer

	// Create an Arrow IPC writer with the table's schema and memory allocator
	writer := ipc.NewWriter(&buf,
		ipc.WithSchema(table.Schema()),
		ipc.WithAllocator(s.pool),
		ipc.WithDictionaryDeltas(true))
	defer writer.Close()

	// Create a record batch reader from the table
	reader := array.NewTableReader(table, 1024) // Process in chunks of 1024 rows
	defer reader.Release()

	// Read and write each batch
	for reader.Next() {
		record := reader.Record()
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to serialize record batch: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeTable converts bytes back to an Arrow Table
// Zero-copy operations are applied where possible during deserialization
func (s *Serializer) DeserializeTable(data []byte) (arrow.Table, error) {
	reader := bytes.NewReader(data)

	// Create an Arrow IPC reader with the specified allocator
	// This enables zero-copy reading where possible
	ipcReader, err := ipc.NewReader(reader,
		ipc.WithAllocator(s.pool),
		// Ensure we use the native endianness for better performance
		ipc.WithEnsureNativeEndian(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer ipcReader.Release()

	// Read all record batches
	var batches []arrow.Record
	for ipcReader.Next() {
		rec := ipcReader.Record()
		rec.Retain() // Important: Ensure the record isn't released when the reader is closed
		batches = append(batches, rec)
	}

	if len(batches) == 0 {
		return nil, fmt.Errorf("no record batches found in data")
	}

	// Create a table from the record batches
	// This maintains the columnar format for efficient processing
	table := array.NewTableFromRecords(batches[0].Schema(), batches)

	// Release the individual batches as they're now owned by the table
	for _, batch := range batches {
		batch.Release()
	}

	return table, nil
}

// CreateSampleTable creates a sample Arrow table for testing and benchmarking
func CreateSampleTable(rows int, pool memory.Allocator) arrow.Table {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Define schema for the sample table
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "category", Type: arrow.BinaryTypes.String},
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
	idBuilder.Reserve(rows)
	valueBuilder.Reserve(rows)
	categoryBuilder.Reserve(rows * 10) // Estimate average string length

	// Add data to the builders
	for i := 0; i < rows; i++ {
		idBuilder.Append(int64(i))
		valueBuilder.Append(float64(i) * 1.5)
		categoryBuilder.Append(fmt.Sprintf("category-%d", i%5))
	}

	// Build the arrays from the builders
	idArray := idBuilder.NewArray()
	defer idArray.Release()
	valueArray := valueBuilder.NewArray()
	defer valueArray.Release()
	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()

	// Create a record batch from the arrays
	batch := array.NewRecord(schema, []arrow.Array{idArray, valueArray, categoryArray}, int64(rows))
	defer batch.Release()

	// Create a table from the record batch
	return array.NewTableFromRecords(schema, []arrow.Record{batch})
}

// CreateSampleRecordBatch creates a sample Arrow RecordBatch for testing and benchmarking
func CreateSampleRecordBatch(rows int, pool memory.Allocator) arrow.Record {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Define schema for the sample record
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "category", Type: arrow.BinaryTypes.String},
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
	idBuilder.Reserve(rows)
	valueBuilder.Reserve(rows)
	categoryBuilder.Reserve(rows * 10) // Estimate average string length

	// Add data to the builders
	for i := 0; i < rows; i++ {
		idBuilder.Append(int64(i))
		valueBuilder.Append(float64(i) * 1.5)
		categoryBuilder.Append(fmt.Sprintf("category-%d", i%5))
	}

	// Build the arrays from the builders
	idArray := idBuilder.NewArray()
	defer idArray.Release()
	valueArray := valueBuilder.NewArray()
	defer valueArray.Release()
	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()

	// Create a record batch from the arrays
	batch := array.NewRecord(schema, []arrow.Array{idArray, valueArray, categoryArray}, int64(rows))

	// Note: We don't release the batch here because the caller is responsible for releasing it
	return batch
}

// FilterTable performs a filtering operation on the table while maintaining columnar format
// This is an example of an efficient vectorized operation on Arrow data
func FilterTable(table arrow.Table, threshold float64, pool memory.Allocator) (arrow.Table, error) {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Get the schema from the input table
	schema := table.Schema()

	// Create builders for the filtered data with pre-allocated capacity
	// This improves performance by reducing reallocations
	estimatedCapacity := int(table.NumRows() / 2) // Estimate half the rows will pass the filter

	idBuilder := array.NewInt64Builder(pool)
	defer idBuilder.Release()
	idBuilder.Reserve(estimatedCapacity)

	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()
	valueBuilder.Reserve(estimatedCapacity)

	categoryBuilder := array.NewStringBuilder(pool)
	defer categoryBuilder.Release()
	categoryBuilder.Reserve(estimatedCapacity * 10) // Estimate for string length

	// Process each column's data in a vectorized manner
	// We'll iterate through all columns in parallel to maintain row alignment

	// Get the chunked arrays for each column
	idChunked := table.Column(0).Data()
	valueChunked := table.Column(1).Data()
	categoryChunked := table.Column(2).Data()

	// Process each chunk
	// For simplicity, we assume all columns have the same chunking structure
	for chunkIdx := 0; chunkIdx < len(valueChunked.Chunks()); chunkIdx++ {
		// Get the chunks for this index (if available)
		if chunkIdx >= len(idChunked.Chunks()) ||
			chunkIdx >= len(valueChunked.Chunks()) ||
			chunkIdx >= len(categoryChunked.Chunks()) {
			continue
		}

		// Get typed arrays for each column in this chunk
		idArray, idOk := idChunked.Chunks()[chunkIdx].(*array.Int64)
		valueArray, valueOk := valueChunked.Chunks()[chunkIdx].(*array.Float64)
		categoryArray, categoryOk := categoryChunked.Chunks()[chunkIdx].(*array.String)

		// Skip if any type assertion failed
		if !idOk || !valueOk || !categoryOk {
			continue
		}

		// Process the chunk in a vectorized manner
		// We iterate once through the chunk and apply the filter condition
		for rowIdx := 0; rowIdx < valueArray.Len(); rowIdx++ {
			// Apply the filter: keep rows where value > threshold
			if valueArray.Value(rowIdx) > threshold {
				idBuilder.Append(idArray.Value(rowIdx))
				valueBuilder.Append(valueArray.Value(rowIdx))
				categoryBuilder.Append(categoryArray.Value(rowIdx))
			}
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
		[]arrow.Array{filteredIdArray, filteredValueArray, filteredCategoryArray},
		int64(idBuilder.Len()))
	defer filteredBatch.Release()

	// Create a table from the filtered batch
	filteredTable := array.NewTableFromRecords(schema, []arrow.Record{filteredBatch})

	return filteredTable, nil
}

// FilterRecordBatch performs a filtering operation on a RecordBatch
// This is optimized for streaming operations
func FilterRecordBatch(batch arrow.Record, threshold float64, pool memory.Allocator) (arrow.Record, error) {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Get the schema from the input batch
	schema := batch.Schema()

	// Print debug info
	fmt.Printf("FilterRecordBatch: batch has %d rows, threshold is %f\n", batch.NumRows(), threshold)

	// Print the first few values to debug
	valueArray, valueOk := batch.Column(1).(*array.Float64)
	if !valueOk {
		return nil, fmt.Errorf("failed to type assert value column")
	}

	fmt.Println("First 10 values in the batch:")
	for i := 0; i < min(10, int(batch.NumRows())); i++ {
		fmt.Printf("  Row %d: value = %f\n", i, valueArray.Value(i))
	}

	// Create builders for the filtered data with pre-allocated capacity
	// This improves performance by reducing reallocations
	estimatedCapacity := int(batch.NumRows() / 2) // Estimate half the rows will pass the filter

	idBuilder := array.NewInt64Builder(pool)
	defer idBuilder.Release()
	idBuilder.Reserve(estimatedCapacity)

	valueBuilder := array.NewFloat64Builder(pool)
	defer valueBuilder.Release()
	valueBuilder.Reserve(estimatedCapacity)

	categoryBuilder := array.NewStringBuilder(pool)
	defer categoryBuilder.Release()
	categoryBuilder.Reserve(estimatedCapacity * 10) // Estimate for string length

	// Get typed arrays for each column
	idArray, idOk := batch.Column(0).(*array.Int64)
	if !idOk {
		return nil, fmt.Errorf("failed to type assert id column")
	}

	categoryArray, categoryOk := batch.Column(2).(*array.String)
	if !categoryOk {
		return nil, fmt.Errorf("failed to type assert category column")
	}

	// Process the batch in a vectorized manner
	// We iterate once through the batch and apply the filter condition
	filteredCount := 0
	for rowIdx := 0; rowIdx < int(batch.NumRows()); rowIdx++ {
		// Apply the filter: keep rows where value > threshold
		if valueArray.Value(rowIdx) > threshold {
			idBuilder.Append(idArray.Value(rowIdx))
			valueBuilder.Append(valueArray.Value(rowIdx))
			categoryBuilder.Append(categoryArray.Value(rowIdx))
			filteredCount++
		}
	}

	fmt.Printf("FilterRecordBatch: filtered %d rows out of %d\n", filteredCount, batch.NumRows())

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
		[]arrow.Array{filteredIdArray, filteredValueArray, filteredCategoryArray},
		int64(filteredCount))

	// Important: We need to retain the batch since we're returning it
	// and the deferred releases above would otherwise release the arrays
	filteredBatch.Retain()

	return filteredBatch, nil
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// BatchProcessor implements the BatchProcessor interface for filtering operations
type FilterBatchProcessor struct {
	threshold float64
	pool      memory.Allocator
}

// NewFilterBatchProcessor creates a new FilterBatchProcessor
func NewFilterBatchProcessor(threshold float64, pool memory.Allocator) *FilterBatchProcessor {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	return &FilterBatchProcessor{
		threshold: threshold,
		pool:      pool,
	}
}

// ProcessBatch processes a single RecordBatch by filtering rows
func (p *FilterBatchProcessor) ProcessBatch(batch arrow.Record, config map[string]string) (arrow.Record, error) {
	// Check if threshold is overridden in the config
	if thresholdStr, ok := config["filterThreshold"]; ok {
		var parsedThreshold float64
		if _, err := fmt.Sscanf(thresholdStr, "%f", &parsedThreshold); err == nil {
			p.threshold = parsedThreshold
		}
	}

	return FilterRecordBatch(batch, p.threshold, p.pool)
}

// Release releases any resources held by the processor
func (p *FilterBatchProcessor) Release() {
	// Nothing to release in this implementation
}

// SampleBatchIterator implements the StreamingBatchIterator interface for testing
type SampleBatchIterator struct {
	pool         memory.Allocator
	batchSize    int
	numBatches   int
	currentBatch int
	schema       *arrow.Schema
}

// NewSampleBatchIterator creates a new SampleBatchIterator
func NewSampleBatchIterator(batchSize int, numBatches int, pool memory.Allocator) *SampleBatchIterator {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Define schema for the sample data
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "category", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	return &SampleBatchIterator{
		pool:         pool,
		batchSize:    batchSize,
		numBatches:   numBatches,
		currentBatch: 0,
		schema:       schema,
	}
}

// Next returns the next batch in the stream
func (it *SampleBatchIterator) Next() (arrow.Record, error) {
	if it.currentBatch >= it.numBatches {
		return nil, nil // No more batches
	}

	// Create a sample batch with an offset based on the current batch
	offset := it.currentBatch * it.batchSize
	batch := it.createBatch(offset)

	it.currentBatch++

	return batch, nil
}

// createBatch creates a sample batch with the specified offset
func (it *SampleBatchIterator) createBatch(offset int) arrow.Record {
	// Create builders for each column
	idBuilder := array.NewInt64Builder(it.pool)
	defer idBuilder.Release()
	valueBuilder := array.NewFloat64Builder(it.pool)
	defer valueBuilder.Release()
	categoryBuilder := array.NewStringBuilder(it.pool)
	defer categoryBuilder.Release()

	// Pre-allocate memory for better performance
	idBuilder.Reserve(it.batchSize)
	valueBuilder.Reserve(it.batchSize)
	categoryBuilder.Reserve(it.batchSize * 10) // Estimate average string length

	// Add data to the builders
	for i := 0; i < it.batchSize; i++ {
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
	batch := array.NewRecord(it.schema, []arrow.Array{idArray, valueArray, categoryArray}, int64(it.batchSize))

	return batch
}

// Release releases any resources held by the iterator
func (it *SampleBatchIterator) Release() {
	// Nothing to release in this implementation
}

// FileBatchWriter implements the BatchWriter interface for writing to a file
type FileBatchWriter struct {
	writer *ipc.FileWriter
	file   io.Closer
}

// NewFileBatchWriter creates a new FileBatchWriter
func NewFileBatchWriter(file io.WriteCloser, schema *arrow.Schema, pool memory.Allocator) (*FileBatchWriter, error) {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}

	// Create an Arrow IPC file writer
	writer, err := ipc.NewFileWriter(file,
		ipc.WithSchema(schema),
		ipc.WithAllocator(pool),
		ipc.WithDictionaryDeltas(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow IPC file writer: %w", err)
	}

	return &FileBatchWriter{
		writer: writer,
		file:   file,
	}, nil
}

// WriteBatch writes a single RecordBatch
func (w *FileBatchWriter) WriteBatch(batch arrow.Record) error {
	return w.writer.Write(batch)
}

// Close closes the writer and releases any resources
func (w *FileBatchWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// GenerateRandomBatch creates a new Arrow batch with random data
func GenerateRandomBatch(batchSize int) (arrow.Record, error) {
	// Create a memory allocator
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
	ids := make([]int32, batchSize)
	values := make([]float64, batchSize)

	for i := 0; i < batchSize; i++ {
		ids[i] = int32(i)
		values[i] = rand.Float64() * 100 // Random value between 0 and 100
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
	batch := array.NewRecord(schema, columns, int64(batchSize))

	return batch, nil
}

// FilterBatch filters an Arrow batch based on a threshold
func FilterBatch(batch arrow.Record, threshold float64) (arrow.Record, error) {
	// Check if the batch has a "value" column
	valueIdx := -1
	for i, field := range batch.Schema().Fields() {
		if field.Name == "value" && field.Type.ID() == arrow.FLOAT64 {
			valueIdx = i
			break
		}
	}

	if valueIdx == -1 {
		return nil, fmt.Errorf("batch does not have a float64 'value' column")
	}

	// Get the value column
	valueCol := batch.Column(valueIdx).(*array.Float64)

	// Create a validity mask
	numRows := int(batch.NumRows())
	validityMask := make([]bool, numRows)
	validCount := 0

	for i := 0; i < numRows; i++ {
		if valueCol.Value(i) > threshold {
			validityMask[i] = true
			validCount++
		}
	}

	// If no rows match, return an empty batch with the same schema
	if validCount == 0 {
		return array.NewRecord(batch.Schema(), make([]arrow.Array, batch.NumCols()), 0), nil
	}

	// Create a new batch with only the valid rows
	allocator := memory.NewGoAllocator()
	builders := make([]array.Builder, batch.NumCols())
	arrays := make([]arrow.Array, batch.NumCols())

	// Initialize builders for each column
	for i, field := range batch.Schema().Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)
		defer builders[i].Release()
	}

	// Copy valid rows to the new batch
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if !validityMask[rowIdx] {
			continue
		}

		// Copy each column's value
		for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
			col := batch.Column(colIdx)
			switch col.DataType().ID() {
			case arrow.INT32:
				builders[colIdx].(*array.Int32Builder).Append(col.(*array.Int32).Value(rowIdx))
			case arrow.FLOAT64:
				builders[colIdx].(*array.Float64Builder).Append(col.(*array.Float64).Value(rowIdx))
			case arrow.STRING:
				builders[colIdx].(*array.StringBuilder).Append(col.(*array.String).Value(rowIdx))
			default:
				return nil, fmt.Errorf("unsupported data type: %s", col.DataType().Name())
			}
		}
	}

	// Build arrays
	for i := 0; i < int(batch.NumCols()); i++ {
		arrays[i] = builders[i].NewArray()
		defer arrays[i].Release()
	}

	// Create the filtered record
	filteredBatch := array.NewRecord(batch.Schema(), arrays, int64(validCount))
	return filteredBatch, nil
}

// CombineBatches combines multiple Arrow batches into a single batch
func CombineBatches(batches []arrow.Record) (arrow.Record, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to combine")
	}

	if len(batches) == 1 {
		// If there's only one batch, just return it
		batches[0].Retain()
		return batches[0], nil
	}

	// Check that all batches have the same schema
	schema := batches[0].Schema()
	for i := 1; i < len(batches); i++ {
		if !schema.Equal(batches[i].Schema()) {
			return nil, fmt.Errorf("batch %d has a different schema", i)
		}
	}

	// Calculate the total number of rows
	var totalRows int64
	for _, batch := range batches {
		totalRows += batch.NumRows()
	}

	// Create builders for each column
	allocator := memory.NewGoAllocator()
	builders := make([]array.Builder, schema.NumFields())
	arrays := make([]arrow.Array, schema.NumFields())

	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)
		defer builders[i].Release()
	}

	// Copy data from each batch
	for _, batch := range batches {
		for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
			col := batch.Column(colIdx)
			builder := builders[colIdx]

			// Copy each value in the column
			for rowIdx := 0; rowIdx < int(batch.NumRows()); rowIdx++ {
				switch col.DataType().ID() {
				case arrow.INT32:
					builder.(*array.Int32Builder).Append(col.(*array.Int32).Value(rowIdx))
				case arrow.FLOAT64:
					builder.(*array.Float64Builder).Append(col.(*array.Float64).Value(rowIdx))
				case arrow.STRING:
					builder.(*array.StringBuilder).Append(col.(*array.String).Value(rowIdx))
				default:
					return nil, fmt.Errorf("unsupported data type: %s", col.DataType().Name())
				}
			}
		}
	}

	// Build arrays
	for i := 0; i < int(schema.NumFields()); i++ {
		arrays[i] = builders[i].NewArray()
		defer arrays[i].Release()
	}

	// Create the combined record
	combinedBatch := array.NewRecord(schema, arrays, totalRows)
	return combinedBatch, nil
}
