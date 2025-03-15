package main

import (
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testInitConfig is a modified version of initConfig that uses a provided FlagSet
// instead of the global pflag to avoid flag redefinition errors in tests
func testInitConfig(t *testing.T, flagSet *pflag.FlagSet, args []string) error {
	// Set up command line flags on the provided FlagSet
	flagSet.String(configConfigFile, "", "Config file path (optional)")
	flagSet.String(configNamespace, defaultNamespace, "Temporal namespace")
	flagSet.String(configTaskQueue, defaultTaskQueue, "Task queue name")
	flagSet.Int(configBatchSize, defaultBatchSize, "Size of each data batch")
	flagSet.Int(configNumBatches, defaultNumBatches, "Number of batches to process")
	flagSet.Float64(configThreshold, defaultThreshold, "Threshold value for filtering")
	flagSet.Int(configWorkerCount, defaultWorkerCount, "Number of worker goroutines")
	flagSet.Bool(configStartWorker, false, "Start a worker")
	flagSet.Bool(configStartWorkflow, false, "Start a workflow")
	flagSet.String(configWorkflowID, "", "Workflow ID (defaults to a generated ID)")
	flagSet.String(configTemporalHost, defaultTemporalHost, "Temporal server host:port")

	// Parse the provided args
	if err := flagSet.Parse(args); err != nil {
		return err
	}

	// Reset viper to ensure a clean state
	viper.Reset()

	// Bind command line flags to viper
	if err := viper.BindPFlags(flagSet); err != nil {
		return err
	}

	// Set environment variable prefix for config
	viper.SetEnvPrefix("ARROW_PIPELINE")

	// Set up environment variable bindings explicitly
	if err := viper.BindEnv(configNamespace, "ARROW_PIPELINE_NAMESPACE"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configTaskQueue, "ARROW_PIPELINE_TASK_QUEUE"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configBatchSize, "ARROW_PIPELINE_BATCH_SIZE"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configNumBatches, "ARROW_PIPELINE_NUM_BATCHES"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configThreshold, "ARROW_PIPELINE_THRESHOLD"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configWorkerCount, "ARROW_PIPELINE_WORKERS"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configStartWorker, "ARROW_PIPELINE_WORKER"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configStartWorkflow, "ARROW_PIPELINE_WORKFLOW"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configWorkflowID, "ARROW_PIPELINE_WORKFLOW_ID"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}
	if err := viper.BindEnv(configTemporalHost, "ARROW_PIPELINE_TEMPORAL_HOST"); err != nil {
		t.Fatalf("Failed to bind environment variable: %v", err)
	}

	viper.AutomaticEnv() // Read environment variables

	// Read config file if specified
	if configFile := viper.GetString(configConfigFile); configFile != "" {
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
	} else if flagSet.Lookup(configConfigFile).Changed {
		// If the config file flag was explicitly set but the value is empty, skip config file
	} else {
		// Look for config file in default locations
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.arrow-pipeline")
		viper.AddConfigPath("/etc/arrow-pipeline")

		// Try to read config file (ignore error if not found)
		_ = viper.ReadInConfig()
	}

	return nil
}

func TestViperConfiguration(t *testing.T) {
	// Save original environment variables to restore later
	origEnv := make(map[string]string)
	for _, env := range []string{
		"ARROW_PIPELINE_NAMESPACE",
		"ARROW_PIPELINE_TASK_QUEUE",
		"ARROW_PIPELINE_BATCH_SIZE",
		"ARROW_PIPELINE_NUM_BATCHES",
		"ARROW_PIPELINE_THRESHOLD",
		"ARROW_PIPELINE_WORKERS",
	} {
		origEnv[env] = os.Getenv(env)
	}

	// Restore environment variables after test
	defer func() {
		for env, val := range origEnv {
			if val == "" {
				os.Unsetenv(env)
			} else {
				os.Setenv(env, val)
			}
		}
	}()

	// Test cases
	testCases := []struct {
		name        string
		envVars     map[string]string
		flagArgs    []string
		configFile  string
		configData  string
		expected    map[string]interface{}
		expectError bool
	}{
		{
			name: "Default Values",
			expected: map[string]interface{}{
				configNamespace:   defaultNamespace,
				configTaskQueue:   defaultTaskQueue,
				configBatchSize:   defaultBatchSize,
				configNumBatches:  defaultNumBatches,
				configThreshold:   defaultThreshold,
				configWorkerCount: defaultWorkerCount,
			},
		},
		{
			name: "Environment Variables",
			envVars: map[string]string{
				"ARROW_PIPELINE_NAMESPACE":   "test-namespace",
				"ARROW_PIPELINE_TASK_QUEUE":  "test-queue",
				"ARROW_PIPELINE_BATCH_SIZE":  "2000",
				"ARROW_PIPELINE_NUM_BATCHES": "20",
				"ARROW_PIPELINE_THRESHOLD":   "750.5",
				"ARROW_PIPELINE_WORKERS":     "10",
			},
			expected: map[string]interface{}{
				configNamespace:   "test-namespace",
				configTaskQueue:   "test-queue",
				configBatchSize:   2000,
				configNumBatches:  20,
				configThreshold:   750.5,
				configWorkerCount: 10,
			},
		},
		{
			name: "Command Line Flags",
			flagArgs: []string{
				"--namespace=flag-namespace",
				"--task-queue=flag-queue",
				"--batch-size=3000",
				"--num-batches=30",
				"--threshold=1000.5",
				"--workers=15",
			},
			expected: map[string]interface{}{
				configNamespace:   "flag-namespace",
				configTaskQueue:   "flag-queue",
				configBatchSize:   3000,
				configNumBatches:  30,
				configThreshold:   1000.5,
				configWorkerCount: 15,
			},
		},
		{
			name:       "Config File",
			configFile: "test-config.yaml",
			configData: `
namespace: file-namespace
task-queue: file-queue
batch-size: 4000
num-batches: 40
threshold: 1500.5
workers: 20
`,
			expected: map[string]interface{}{
				configNamespace:   "file-namespace",
				configTaskQueue:   "file-queue",
				configBatchSize:   4000,
				configNumBatches:  40,
				configThreshold:   1500.5,
				configWorkerCount: 20,
			},
		},
		{
			name: "Priority: Flags > Env > File > Default",
			envVars: map[string]string{
				"ARROW_PIPELINE_NAMESPACE":  "env-namespace",
				"ARROW_PIPELINE_TASK_QUEUE": "env-queue",
				"ARROW_PIPELINE_BATCH_SIZE": "5000",
			},
			flagArgs: []string{
				"--namespace=flag-namespace",
				"--batch-size=6000",
			},
			configFile: "test-config.yaml",
			configData: `
namespace: file-namespace
task-queue: file-queue
batch-size: 4000
num-batches: 40
threshold: 1500.5
workers: 20
`,
			expected: map[string]interface{}{
				configNamespace:   "flag-namespace", // From flag
				configTaskQueue:   "env-queue",      // From env
				configBatchSize:   6000,             // From flag
				configNumBatches:  40,               // From file
				configThreshold:   1500.5,           // From file
				configWorkerCount: 20,               // From file
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset viper for each test case
			viper.Reset()

			// Clear environment variables
			for env := range origEnv {
				os.Unsetenv(env)
			}

			// Set environment variables for this test
			for env, val := range tc.envVars {
				os.Setenv(env, val)
			}

			// Create config file if needed
			if tc.configFile != "" {
				err := os.WriteFile(tc.configFile, []byte(tc.configData), 0644)
				require.NoError(t, err)
				defer os.Remove(tc.configFile)

				// For config file tests, explicitly set the config file flag
				if tc.name == "Config File" || tc.name == "Priority: Flags > Env > File > Default" {
					if tc.flagArgs == nil {
						tc.flagArgs = []string{}
					}
					tc.flagArgs = append(tc.flagArgs, "--config="+tc.configFile)
				}
			}

			// Create a new FlagSet for this test case
			flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)

			// Initialize configuration with the test FlagSet
			err := testInitConfig(t, flagSet, tc.flagArgs)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Check expected values
			for key, expectedValue := range tc.expected {
				switch v := expectedValue.(type) {
				case string:
					assert.Equal(t, v, viper.GetString(key), "Key: %s", key)
				case int:
					assert.Equal(t, v, viper.GetInt(key), "Key: %s", key)
				case float64:
					assert.Equal(t, v, viper.GetFloat64(key), "Key: %s", key)
				case bool:
					assert.Equal(t, v, viper.GetBool(key), "Key: %s", key)
				default:
					t.Fatalf("Unsupported type for key %s", key)
				}
			}
		})
	}
}

func TestGetGrpcOptions(t *testing.T) {
	// Test that the function returns valid gRPC options
	options := getGrpcOptions()
	assert.NotNil(t, options)
	assert.Greater(t, len(options), 0)
}
