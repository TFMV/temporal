package arrow

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
)

// BenchmarkSerialization benchmarks the serialization and deserialization of Arrow tables
// to measure the efficiency of zero-copy operations
func BenchmarkSerialization(b *testing.B) {
	// Create a memory allocator
	allocator := memory.NewGoAllocator()

	// Create a serializer
	serializer := NewSerializer(allocator)

	// Create sample tables of different sizes for benchmarking
	tableSizes := []int{1000, 10000, 100000}

	for _, size := range tableSizes {
		b.Run("table_size_"+string(rune(size)), func(b *testing.B) {
			// Create a sample table
			table := CreateSampleTable(size, allocator)
			defer table.Release()

			// Reset the timer before the benchmark loop
			b.ResetTimer()

			// Measure the time taken to serialize and deserialize the table
			for i := 0; i < b.N; i++ {
				// Serialize the table
				data, err := serializer.SerializeTable(table)
				if err != nil {
					b.Fatalf("Failed to serialize table: %v", err)
				}

				// Deserialize the table
				deserializedTable, err := serializer.DeserializeTable(data)
				if err != nil {
					b.Fatalf("Failed to deserialize table: %v", err)
				}
				deserializedTable.Release()
			}
		})
	}
}

// BenchmarkFiltering benchmarks the filtering operation on Arrow tables
// to measure the performance of columnar processing
func BenchmarkFiltering(b *testing.B) {
	// Create a memory allocator
	allocator := memory.NewGoAllocator()

	// Create sample tables of different sizes for benchmarking
	tableSizes := []int{1000, 10000, 100000}

	for _, size := range tableSizes {
		b.Run("filter_rows_"+string(rune(size)), func(b *testing.B) {
			// Create a sample table
			table := CreateSampleTable(size, allocator)
			defer table.Release()

			// Reset the timer before the benchmark loop
			b.ResetTimer()

			// Measure the time taken to filter the table
			for i := 0; i < b.N; i++ {
				// Filter the table using vectorized operations
				filteredTable, err := FilterTable(table, 5.0, allocator)
				if err != nil {
					b.Fatalf("Failed to filter table: %v", err)
				}
				filteredTable.Release()
			}
		})
	}
}

// BenchmarkEndToEndPipeline benchmarks the entire pipeline process
// from ingestion through processing to storage
func BenchmarkEndToEndPipeline(b *testing.B) {
	// Create a memory allocator
	allocator := memory.NewGoAllocator()

	// Create a serializer
	serializer := NewSerializer(allocator)

	// Create sample tables of different sizes for benchmarking
	tableSizes := []int{1000, 10000, 100000}

	for _, size := range tableSizes {
		b.Run("pipeline_"+string(rune(size)), func(b *testing.B) {
			// Reset the timer before the benchmark loop
			b.ResetTimer()

			// Measure the time taken for the entire pipeline
			for i := 0; i < b.N; i++ {
				// 1. Ingestion - Create a sample table
				table := CreateSampleTable(size, allocator)

				// 2. Serialization - For passing between activities
				data, err := serializer.SerializeTable(table)
				if err != nil {
					b.Fatalf("Failed to serialize table: %v", err)
				}
				table.Release()

				// 3. Deserialization - In the processing activity
				table, err = serializer.DeserializeTable(data)
				if err != nil {
					b.Fatalf("Failed to deserialize table: %v", err)
				}

				// 4. Processing - Filter the data
				filteredTable, err := FilterTable(table, 5.0, allocator)
				if err != nil {
					b.Fatalf("Failed to filter table: %v", err)
				}
				table.Release()

				// 5. Serialization - For passing to storage activity
				data, err = serializer.SerializeTable(filteredTable)
				if err != nil {
					b.Fatalf("Failed to serialize filtered table: %v", err)
				}
				filteredTable.Release()

				// 6. Deserialization - In the storage activity
				table, err = serializer.DeserializeTable(data)
				if err != nil {
					b.Fatalf("Failed to deserialize table: %v", err)
				}
				table.Release()
			}
		})
	}
}
