package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/TFMV/temporal/pkg/workflow"
)

const (
	// Configuration keys
	configNamespace     = "namespace"
	configTaskQueue     = "task-queue"
	configBatchSize     = "batch-size"
	configNumBatches    = "num-batches"
	configThreshold     = "threshold"
	configWorkerCount   = "workers"
	configStartWorker   = "worker"
	configStartWorkflow = "workflow"
	configWorkflowID    = "workflow-id"
	configTemporalHost  = "temporal-host"
	configConfigFile    = "config"

	// Default values
	defaultNamespace    = "default"
	defaultTaskQueue    = "arrow-pipeline"
	defaultBatchSize    = 1000
	defaultNumBatches   = 10
	defaultThreshold    = 500.0
	defaultWorkerCount  = 5
	defaultTemporalHost = "localhost:7233"
)

func main() {
	// Initialize configuration
	if err := initConfig(); err != nil {
		log.Fatalf("Failed to initialize configuration: %v", err)
	}

	// Create a Temporal client with gRPC options
	c, err := client.Dial(client.Options{
		Namespace: viper.GetString(configNamespace),
		HostPort:  viper.GetString(configTemporalHost),
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: getGrpcOptions(),
		},
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
	if viper.GetBool(configStartWorker) {
		startArrowWorker(ctx, c, viper.GetString(configTaskQueue), viper.GetInt(configWorkerCount))
	}

	// Start workflow if requested
	if viper.GetBool(configStartWorkflow) {
		id := viper.GetString(configWorkflowID)
		if id == "" {
			id = fmt.Sprintf("arrow-pipeline-%v", time.Now().UnixNano())
		}
		startArrowWorkflow(ctx, c, id,
			viper.GetString(configTaskQueue),
			viper.GetInt(configBatchSize),
			viper.GetInt(configNumBatches),
			viper.GetFloat64(configThreshold))
	}

	// If neither worker nor workflow was started, print usage
	if !viper.GetBool(configStartWorker) && !viper.GetBool(configStartWorkflow) {
		pflag.Usage()
		os.Exit(1)
	}

	// Wait for context cancellation (from signal handler)
	<-ctx.Done()
	log.Println("Shutdown complete")
}

// initConfig initializes the configuration using Viper
func initConfig() error {
	// Set up command line flags
	pflag.String(configConfigFile, "", "Config file path (optional)")
	pflag.String(configNamespace, defaultNamespace, "Temporal namespace")
	pflag.String(configTaskQueue, defaultTaskQueue, "Task queue name")
	pflag.Int(configBatchSize, defaultBatchSize, "Size of each data batch")
	pflag.Int(configNumBatches, defaultNumBatches, "Number of batches to process")
	pflag.Float64(configThreshold, defaultThreshold, "Threshold value for filtering")
	pflag.Int(configWorkerCount, defaultWorkerCount, "Number of worker goroutines")
	pflag.Bool(configStartWorker, false, "Start a worker")
	pflag.Bool(configStartWorkflow, false, "Start a workflow")
	pflag.String(configWorkflowID, "", "Workflow ID (defaults to a generated ID)")
	pflag.String(configTemporalHost, defaultTemporalHost, "Temporal server host:port")
	pflag.Parse()

	// Bind command line flags to viper
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return fmt.Errorf("failed to bind flags: %w", err)
	}

	// Set environment variable prefix for config
	viper.SetEnvPrefix("ARROW_PIPELINE")
	viper.AutomaticEnv() // Read environment variables

	// Read config file if specified
	if configFile := viper.GetString(configConfigFile); configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	} else {
		// Look for config file in default locations
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.arrow-pipeline")
		viper.AddConfigPath("/etc/arrow-pipeline")

		// Try to read config file (ignore error if not found)
		if err := viper.ReadInConfig(); err == nil {
			log.Printf("Using config file: %s", viper.ConfigFileUsed())
		}
	}

	return nil
}

// getGrpcOptions returns gRPC dial options optimized for high-performance data transfer
func getGrpcOptions() []grpc.DialOption {
	// Set up gRPC keepalive parameters
	kaParams := keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             5 * time.Second,  // wait 5 seconds for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}

	// Set up gRPC options
	// For production, you would use proper TLS credentials instead of insecure
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024), // 64MB max message size for large data transfers
			grpc.MaxCallSendMsgSize(64*1024*1024), // 64MB max message size for large data transfers
		),
		// Add custom interceptors if needed
		// grpc.WithUnaryInterceptor(yourCustomInterceptor),
	}
}

// startArrowWorker starts a Temporal worker for the Arrow pipeline
func startArrowWorker(ctx context.Context, c client.Client, taskQueue string, workerCount int) {
	log.Printf("Starting Arrow pipeline worker on task queue: %s", taskQueue)

	// Create a worker with optimized options for data processing
	w := worker.New(c, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: workerCount,
		// Optimize for large data payloads
		MaxConcurrentWorkflowTaskExecutionSize: 50,
		// Enable sticky execution for better performance
		StickyScheduleToStartTimeout: time.Second * 5,
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

	// Create workflow parameters
	params := workflow.StreamingWorkflowParams{
		BatchSize:  batchSize,
		NumBatches: numBatches,
		Threshold:  threshold,
	}

	// Workflow options optimized for large data processing
	options := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
		// Set a reasonable timeout for the entire workflow
		WorkflowExecutionTimeout: time.Hour,
		// Set a reasonable timeout for each workflow task
		WorkflowTaskTimeout: time.Minute,
		// Enable memo for workflow metadata
		Memo: map[string]interface{}{
			"batchSize":  batchSize,
			"numBatches": numBatches,
			"threshold":  threshold,
		},
	}

	// Start the workflow with the params struct
	we, err := c.ExecuteWorkflow(ctx, options, workflow.StreamingWorkflow, params)
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
