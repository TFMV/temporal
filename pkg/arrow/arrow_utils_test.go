package arrow

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializerRoundTrip(t *testing.T) {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a serializer
	serializer := NewSerializer(pool)

	// Create a sample record batch
	batch := CreateSampleRecordBatch(100, pool)
	defer batch.Release()

	// Serialize the batch
	data, err := serializer.SerializeRecord(batch)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize the batch
	deserializedBatch, err := serializer.DeserializeRecord(data)
	require.NoError(t, err)
	defer deserializedBatch.Release()

	// Verify the deserialized batch
	assert.Equal(t, batch.NumRows(), deserializedBatch.NumRows())
	assert.Equal(t, batch.NumCols(), deserializedBatch.NumCols())

	// Check schema equality
	assert.True(t, batch.Schema().Equal(deserializedBatch.Schema()))

	// Check data equality for each column
	for i := 0; i < int(batch.NumCols()); i++ {
		assert.True(t, array.Equal(batch.Column(i), deserializedBatch.Column(i)))
	}
}

func TestSerializerTableRoundTrip(t *testing.T) {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a serializer
	serializer := NewSerializer(pool)

	// Create a sample record batch instead of a table
	// This avoids issues with the table implementation
	batch := CreateSampleRecordBatch(100, pool)
	defer batch.Release()

	// Create a table from the record batch
	table := array.NewTableFromRecords(batch.Schema(), []arrow.Record{batch})
	defer table.Release()

	// Serialize the table
	data, err := serializer.SerializeTable(table)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize the table
	deserializedTable, err := serializer.DeserializeTable(data)
	require.NoError(t, err)
	defer deserializedTable.Release()

	// Verify the deserialized table
	assert.Equal(t, table.NumRows(), deserializedTable.NumRows())
	assert.Equal(t, table.NumCols(), deserializedTable.NumCols())

	// Check schema equality
	assert.True(t, table.Schema().Equal(deserializedTable.Schema()))
}

func TestFilterRecordBatch(t *testing.T) {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a sample record batch with known values
	// We'll create a batch with 100 rows where each row has value = row_index * 1.5
	batch := CreateSampleRecordBatch(100, pool)
	defer batch.Release()

	// Define test cases with different thresholds
	testCases := []struct {
		name      string
		threshold float64
	}{
		{"NoFiltering", -1.0},    // All rows should pass (all values are >= 0)
		{"HalfFiltering", 75.0},  // Rows with index >= 50 pass (value >= 75.0)
		{"MostFiltering", 140.0}, // Only rows with index >= 94 pass (value >= 141.0)
		{"AllFiltering", 1000.0}, // No rows pass (max value is 148.5)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Filter the batch
			filteredBatch, err := FilterRecordBatch(batch, tc.threshold, pool)
			require.NoError(t, err)
			defer filteredBatch.Release()

			// Get the number of rows in the filtered batch
			filteredRows := filteredBatch.NumRows()
			t.Logf("Filtered batch has %d rows with threshold %f", filteredRows, tc.threshold)

			// If we have rows, verify they all pass the filter
			if filteredRows > 0 {
				valueArray, ok := filteredBatch.Column(1).(*array.Float64)
				require.True(t, ok)

				// Check that all values in the filtered batch are greater than the threshold
				for i := 0; i < valueArray.Len(); i++ {
					value := valueArray.Value(i)
					assert.Greater(t, value, tc.threshold,
						"Row %d has value %f which is not > %f", i, value, tc.threshold)
				}

				// For the first test case (NoFiltering), verify we kept all rows
				if tc.name == "NoFiltering" {
					assert.Equal(t, batch.NumRows(), filteredRows,
						"NoFiltering should keep all rows")
				}

				// For AllFiltering, we should have 0 rows
				if tc.name == "AllFiltering" {
					assert.Equal(t, int64(0), filteredRows,
						"AllFiltering should filter out all rows")
				}
			} else {
				// If we have no rows, the threshold should be high enough to filter everything
				// Get the max value from the original batch
				valueArray, ok := batch.Column(1).(*array.Float64)
				require.True(t, ok)

				maxValue := float64(-1)
				for i := 0; i < valueArray.Len(); i++ {
					if valueArray.Value(i) > maxValue {
						maxValue = valueArray.Value(i)
					}
				}

				assert.Greater(t, tc.threshold, maxValue,
					"Threshold %f should be greater than max value %f to filter all rows",
					tc.threshold, maxValue)
			}
		})
	}
}

func TestBatchProcessor(t *testing.T) {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a sample record batch with known values
	// We'll create a batch with 100 rows where each row has value = row_index * 1.5
	batch := CreateSampleRecordBatch(100, pool)
	defer batch.Release()

	// Create a batch processor with threshold 50.0
	// This should keep rows with index >= 34 (value >= 51.0)
	processor := NewFilterBatchProcessor(50.0, pool)
	defer processor.Release()

	// Process the batch
	filteredBatch, err := processor.ProcessBatch(batch, nil)
	require.NoError(t, err)
	defer filteredBatch.Release()

	// Verify the filtered batch has rows
	filteredRows := filteredBatch.NumRows()
	t.Logf("Filtered batch has %d rows with threshold 50.0", filteredRows)
	assert.Greater(t, filteredRows, int64(0), "Filtered batch should have rows")

	// Verify all values in the filtered batch are > 50.0
	valueArray, ok := filteredBatch.Column(1).(*array.Float64)
	require.True(t, ok)
	for i := 0; i < valueArray.Len(); i++ {
		assert.Greater(t, valueArray.Value(i), 50.0,
			"Row %d has value %f which is not > 50.0", i, valueArray.Value(i))
	}

	// Test with config override - threshold 75.0
	// This should keep rows with index >= 50 (value >= 75.0)
	config := map[string]string{
		"filterThreshold": "75.0",
	}

	filteredBatch2, err := processor.ProcessBatch(batch, config)
	require.NoError(t, err)
	defer filteredBatch2.Release()

	// Verify the filtered batch with overridden threshold
	filteredRows2 := filteredBatch2.NumRows()
	t.Logf("Filtered batch has %d rows with threshold 75.0", filteredRows2)
	assert.Greater(t, filteredRows2, int64(0), "Filtered batch should have rows")

	// Verify all values in the filtered batch are > 75.0
	valueArray2, ok := filteredBatch2.Column(1).(*array.Float64)
	require.True(t, ok)
	for i := 0; i < valueArray2.Len(); i++ {
		assert.Greater(t, valueArray2.Value(i), 75.0,
			"Row %d has value %f which is not > 75.0", i, valueArray2.Value(i))
	}

	// The second filtered batch should have fewer rows than the first
	assert.Less(t, filteredRows2, filteredRows,
		"Filtered batch with higher threshold should have fewer rows")
}

func TestSampleBatchIterator(t *testing.T) {
	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a batch iterator
	batchSize := 10
	numBatches := 5
	iterator := NewSampleBatchIterator(batchSize, numBatches, pool)
	defer iterator.Release()

	// Iterate through all batches
	batchCount := 0
	totalRows := 0

	for {
		batch, err := iterator.Next()
		require.NoError(t, err)

		if batch == nil {
			break // No more batches
		}

		// Verify the batch
		assert.Equal(t, int64(batchSize), batch.NumRows())
		assert.Equal(t, int64(3), batch.NumCols()) // id, value, category

		// Check that the batch has the expected offset
		idArray, ok := batch.Column(0).(*array.Int64)
		require.True(t, ok)
		assert.Equal(t, int64(batchCount*batchSize), idArray.Value(0)) // First ID should match the offset

		totalRows += int(batch.NumRows())
		batchCount++

		batch.Release() // Release the batch
	}

	// Verify we got the expected number of batches and rows
	assert.Equal(t, numBatches, batchCount)
	assert.Equal(t, batchSize*numBatches, totalRows)
}

func TestFileBatchWriter(t *testing.T) {
	// Skip this test in short mode as it involves file I/O
	if testing.Short() {
		t.Skip("Skipping file I/O test in short mode")
	}

	// Create a memory allocator
	pool := memory.NewGoAllocator()

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "arrow-test-*.arrow")
	require.NoError(t, err)
	tempFileName := tempFile.Name()
	tempFile.Close() // Close it so we can reopen it properly
	defer os.Remove(tempFileName)

	// Create a sample record batch
	batch := CreateSampleRecordBatch(100, pool)
	defer batch.Release()

	// Open the file for writing
	file, err := os.Create(tempFileName)
	require.NoError(t, err)

	// Create a file batch writer
	writer, err := NewFileBatchWriter(file, batch.Schema(), pool)
	require.NoError(t, err)

	// Write the batch
	err = writer.WriteBatch(batch)
	require.NoError(t, err)

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	// Verify the file was written
	fileInfo, err := os.Stat(tempFileName)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0))

	// Read the file back to verify
	file, err = os.Open(tempFileName)
	require.NoError(t, err)
	defer file.Close()

	// Create an Arrow IPC reader
	reader, err := ipc.NewFileReader(file, ipc.WithAllocator(pool))
	require.NoError(t, err)
	defer reader.Close()

	// Verify the number of record batches
	assert.Equal(t, 1, reader.NumRecords())

	// Read the record batch
	readBatch, err := reader.Record(0)
	require.NoError(t, err)
	defer readBatch.Release()

	// Verify the batch contents
	assert.Equal(t, batch.NumRows(), readBatch.NumRows())
	assert.Equal(t, batch.NumCols(), readBatch.NumCols())
}
