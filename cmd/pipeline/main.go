package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/TFMV/temporal/pkg/workflow"
)

const (
	// Default values for command line flags
	defaultNamespace   = "default"
	defaultTaskQueue   = "arrow-pipeline"
	defaultBatchSize   = 1000
	defaultNumBatches  = 10
	defaultThreshold   = 500.0
	defaultWorkerCount = 5
)

func main() {
	// Parse command line flags
	namespace := flag.String("namespace", defaultNamespace, "Temporal namespace")
	taskQueue := flag.String("task-queue", defaultTaskQueue, "Task queue name")
	batchSize := flag.Int("batch-size", defaultBatchSize, "Size of each data batch")
	numBatches := flag.Int("num-batches", defaultNumBatches, "Number of batches to process")
	threshold := flag.Float64("threshold", defaultThreshold, "Threshold value for filtering")
	workerCount := flag.Int("workers", defaultWorkerCount, "Number of worker goroutines")
	startWorker := flag.Bool("worker", false, "Start a worker")
	startWorkflow := flag.Bool("workflow", false, "Start a workflow")
	workflowID := flag.String("workflow-id", "", "Workflow ID (defaults to a generated ID)")
	flag.Parse()

	// Create a Temporal client
	c, err := client.Dial(client.Options{
		Namespace: *namespace,
	})
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		log.Println("Shutdown signal received, stopping...")
		cancel()
	}()

	// Start worker if requested
	if *startWorker {
		startArrowWorker(ctx, c, *taskQueue, *workerCount)
	}

	// Start workflow if requested
	if *startWorkflow {
		id := *workflowID
		if id == "" {
			id = fmt.Sprintf("arrow-pipeline-%v", time.Now().UnixNano())
		}
		startArrowWorkflow(ctx, c, id, *taskQueue, *batchSize, *numBatches, *threshold)
	}

	// If neither worker nor workflow was started, print usage
	if !*startWorker && !*startWorkflow {
		flag.Usage()
		os.Exit(1)
	}

	// Wait for context cancellation (from signal handler)
	<-ctx.Done()
	log.Println("Shutdown complete")
}

// startArrowWorker starts a Temporal worker for the Arrow pipeline
func startArrowWorker(ctx context.Context, c client.Client, taskQueue string, workerCount int) {
	log.Printf("Starting Arrow pipeline worker on task queue: %s", taskQueue)

	// Create a worker
	w := worker.New(c, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: workerCount,
	})

	// Register workflow and activities
	workflow.RegisterStreamingWorkflow(w)

	// Start the worker (non-blocking)
	err := w.Start()
	if err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	// Set up cleanup on context cancellation
	go func() {
		<-ctx.Done()
		log.Println("Stopping worker...")
		w.Stop()
		log.Println("Worker stopped")
	}()

	log.Println("Worker started successfully")
}

// startArrowWorkflow starts the Arrow streaming workflow
func startArrowWorkflow(ctx context.Context, c client.Client, workflowID, taskQueue string, batchSize, numBatches int, threshold float64) {
	log.Printf("Starting Arrow streaming workflow (ID: %s)", workflowID)

	// Workflow options
	options := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
		// Set a reasonable timeout for the entire workflow
		WorkflowExecutionTimeout: time.Hour,
	}

	// Start the workflow
	we, err := c.ExecuteWorkflow(ctx, options, workflow.StreamingWorkflow, batchSize, numBatches, threshold)
	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	log.Printf("Workflow started with Run ID: %s", we.GetRunID())

	// Wait for workflow completion (optional)
	var result int
	err = we.Get(ctx, &result)
	if err != nil {
		log.Printf("Workflow execution failed: %v", err)
	} else {
		log.Printf("Workflow completed successfully. Processed %d rows.", result)
	}
}
