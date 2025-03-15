package workflow

import (
	"context"
	"os"
	"testing"

	arrowpkg "github.com/TFMV/temporal/pkg/arrow"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// shouldSkipTemporalTests returns true if Temporal tests should be skipped
// This can be controlled by setting the SKIP_TEMPORAL_TESTS environment variable
func shouldSkipTemporalTests() bool {
	// Skip by default unless explicitly enabled
	return os.Getenv("ENABLE_TEMPORAL_TESTS") != "true"
}

func TestStreamingWorkflow(t *testing.T) {
	if shouldSkipTemporalTests() {
		t.Skip("Skipping Temporal workflow test - set ENABLE_TEMPORAL_TESTS=true to enable")
	}

	// Create test suite
	ts := testsuite.WorkflowTestSuite{}

	// Create a custom data converter for Arrow types
	pool := memory.NewGoAllocator()
	arrowConverter := arrowpkg.NewArrowDataConverter(pool)

	// Create a test environment with the custom data converter
	env := ts.NewTestWorkflowEnvironment()
	env.SetDataConverter(arrowConverter)

	// Register workflow and activities
	env.RegisterWorkflow(StreamingWorkflow)
	env.RegisterActivity(GenerateBatchActivity)
	env.RegisterActivity(ProcessBatchActivity)
	env.RegisterActivity(StoreBatchActivity)

	// Mock the activities to avoid actual execution
	env.OnActivity(GenerateBatchActivity, mock.Anything, mock.Anything, mock.Anything).Return(
		arrowpkg.CreateSampleRecordBatch(10, pool), nil)

	env.OnActivity(ProcessBatchActivity, mock.Anything, mock.Anything, mock.Anything).Return(
		arrowpkg.CreateSampleRecordBatch(5, pool), nil)

	env.OnActivity(StoreBatchActivity, mock.Anything, mock.Anything, mock.Anything).Return(5, nil)

	// Set test parameters
	params := StreamingWorkflowParams{
		BatchSize:  10,
		NumBatches: 3,
		Threshold:  5.0,
	}

	// Execute workflow
	env.ExecuteWorkflow(StreamingWorkflow, params)

	// Verify workflow completed successfully
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	// Get workflow result
	var result int
	require.NoError(t, env.GetWorkflowResult(&result))

	// Verify result - should be the total number of processed rows
	// Each batch has 10 rows, 3 batches, and roughly half should pass the filter
	assert.Equal(t, 15, result) // 3 batches * 5 rows each
}

func TestGenerateBatchActivity(t *testing.T) {
	// Skip this test if we're running in short mode
	if testing.Short() {
		t.Skip("Skipping activity test in short mode")
	}

	// Skip if Temporal tests are disabled
	if shouldSkipTemporalTests() {
		t.Skip("Skipping Temporal activity test - set ENABLE_TEMPORAL_TESTS=true to enable")
	}

	// Create test suite
	ts := testsuite.WorkflowTestSuite{}

	// Create a custom data converter for Arrow types
	pool := memory.NewGoAllocator()
	arrowConverter := arrowpkg.NewArrowDataConverter(pool)

	// Create a test environment with the custom data converter
	env := ts.NewTestActivityEnvironment()
	env.SetDataConverter(arrowConverter)

	// Register activity
	env.RegisterActivity(GenerateBatchActivity)

	// Set up context
	ctx := context.Background()

	// Execute activity
	batch, err := GenerateBatchActivity(ctx, 10, 0)
	require.NoError(t, err)
	defer batch.Release()

	// Verify batch
	assert.Equal(t, int64(10), batch.NumRows())
	assert.Equal(t, int64(3), batch.NumCols()) // id, value, category
}

func TestProcessBatchActivity(t *testing.T) {
	// Skip this test if we're running in short mode
	if testing.Short() {
		t.Skip("Skipping activity test in short mode")
	}

	// Skip if Temporal tests are disabled
	if shouldSkipTemporalTests() {
		t.Skip("Skipping Temporal activity test - set ENABLE_TEMPORAL_TESTS=true to enable")
	}

	// Create test suite
	ts := testsuite.WorkflowTestSuite{}

	// Create a custom data converter for Arrow types
	pool := memory.NewGoAllocator()
	arrowConverter := arrowpkg.NewArrowDataConverter(pool)

	// Create a test environment with the custom data converter
	env := ts.NewTestActivityEnvironment()
	env.SetDataConverter(arrowConverter)

	// Register activity
	env.RegisterActivity(ProcessBatchActivity)

	// Set up context
	ctx := context.Background()

	// Create a batch to process
	pool = memory.NewGoAllocator()
	batch := createTestBatch(10, 0, pool)
	defer batch.Release()

	// Execute activity
	processedBatch, err := ProcessBatchActivity(ctx, batch, 5.0)
	require.NoError(t, err)
	defer processedBatch.Release()

	// Verify processed batch
	assert.LessOrEqual(t, processedBatch.NumRows(), batch.NumRows())
	assert.Equal(t, batch.NumCols(), processedBatch.NumCols())
}

func TestStoreBatchActivity(t *testing.T) {
	// Skip this test if we're running in short mode
	if testing.Short() {
		t.Skip("Skipping activity test in short mode")
	}

	// Skip if Temporal tests are disabled
	if shouldSkipTemporalTests() {
		t.Skip("Skipping Temporal activity test - set ENABLE_TEMPORAL_TESTS=true to enable")
	}

	// Create test suite
	ts := testsuite.WorkflowTestSuite{}

	// Create a custom data converter for Arrow types
	pool := memory.NewGoAllocator()
	arrowConverter := arrowpkg.NewArrowDataConverter(pool)

	// Create a test environment with the custom data converter
	env := ts.NewTestActivityEnvironment()
	env.SetDataConverter(arrowConverter)

	// Register activity
	env.RegisterActivity(StoreBatchActivity)

	// Set up context
	ctx := context.Background()

	// Create a batch to store
	pool = memory.NewGoAllocator()
	batch := createTestBatch(10, 0, pool)
	defer batch.Release()

	// Execute activity with a test output path
	count, err := StoreBatchActivity(ctx, batch, "test-output")
	require.NoError(t, err)

	// Verify count
	assert.Equal(t, int(batch.NumRows()), count)
}

func TestRegisterStreamingWorkflow(t *testing.T) {
	// Skip if Temporal tests are disabled
	if shouldSkipTemporalTests() {
		t.Skip("Skipping Temporal registration test - set ENABLE_TEMPORAL_TESTS=true to enable")
	}

	// Create a worker
	w := worker.New(nil, "test-queue", worker.Options{})

	// Register workflow and activities
	RegisterStreamingWorkflow(w)

	// No assertions needed - if registration fails, it will panic
}

// Helper function to create a test batch
func createTestBatch(rows int, offset int, pool memory.Allocator) arrow.Record {
	// Create a sample record batch using the arrow package
	return arrowpkg.CreateSampleRecordBatch(rows, pool)
}
