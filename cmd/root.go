// Copyright 2020 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"github.com/coinbase/rosetta-sdk-go/types"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	// configEnvKey is an env variable name that sets a config file location
	configEnvKey = "ROSETTA_CONFIGURATION_FILE"
)

var (
	rootCmd = &cobra.Command{
		Use:               "rosetta-cli",
		Short:             "CLI for the Rosetta API",
		PersistentPreRunE: rootPreRun,
	}

	configurationFile      string
	cpuProfile             string
	memProfile             string
	blockProfile           string
	onlineURL              string
	offlineURL             string
	startIndex             int64
	endIndex               int64
	dataResultFile         string
	constructionResultFile string
	dataDirectory          string
	inMemoryMode           bool
	tableSize              int64
	requestUUID            string
	statusPort             uint
	InfoMetaData           string
	targetAccount          string

	// Config is the populated *configuration.Configuration from
	// the configurationFile. If none is provided, this is set
	// to the default settings.
	Config *configuration.Configuration

	// Context is the context to use for this invocation of the cli.
	Context context.Context

	// SignalReceived is set to true when a signal causes us to exit. This makes
	// determining the error message to show on exit much more easy.
	SignalReceived = false

	// cpuProfileCleanup is called after the root command is executed to
	// cleanup a running cpu profile.
	cpuProfileCleanup func()

	// blockProfileCleanup is called after the root command is executed to
	// cleanup a running block profile.
	blockProfileCleanup func()

	// OnlyChanges is a boolean indicating if only the balance changes should be
	// logged to the console.
	OnlyChanges bool

	// allSpecs is a boolean indicating whether check:spec should verify only Coinbase
	// spec requirements, or the minimum requirements as well.
	checkAllSpecs bool

	// If non-empty, used to validate that /network/options matches the contents of the file
	// located at this path. The intended use case is someone previously ran
	// utils:asserter-configuration `asserterConfigurationFile`, so the validation is being done
	// against the same file (or an identical copy of that file at this path). This file contains a
	// snapshot of /network/options (and some other unrelated configuration options), so this will
	// trigger validation of the current /network/options against that. See
	// utils:asserter-configuration for more details.
	//
	// The main goal is to verify these core configuration options don't change across releases,
	// which has caused production incidents in the past. This can be used for both check:data
	// and check:construction.
	asserterConfigurationFile string

	// curveType is used to specify curve type to generate a keypair using rosetta-cli key:gen
	// command
	curveType string
)

// rootPreRun is executed before the root command runs and sets up cpu
// profiling.
//
// Based on https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
func rootPreRun(*cobra.Command, []string) error {
	if cpuProfile != "" {
		f, err := os.Create(path.Clean(cpuProfile))
		if err != nil {
			return fmt.Errorf("unable to create CPU profile file: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			if err := f.Close(); err != nil {
				log.Printf("error while closing cpu profile file: %v\n", err)
			}
			return err
		}

		cpuProfileCleanup = func() {
			pprof.StopCPUProfile()
			if err := f.Close(); err != nil {
				log.Printf("error while closing cpu profile file: %v\n", err)
			}
		}
	}

	if blockProfile != "" {
		runtime.SetBlockProfileRate(1)
		f, err := os.Create(path.Clean(blockProfile))
		if err != nil {
			return fmt.Errorf("unable to create block profile file: %w", err)
		}

		p := pprof.Lookup("block")
		blockProfileCleanup = func() {
			if err := p.WriteTo(f, 0); err != nil {
				log.Printf("error while writing block profile file: %v\n", err)
			}
			if err := f.Close(); err != nil {
				log.Printf("error while closing block profile file: %v\n", err)
			}
		}
	}

	return nil
}

// rootPostRun is executed after the root command runs and performs memory
// profiling.
func rootPostRun() {
	if cpuProfileCleanup != nil {
		cpuProfileCleanup()
	}

	if blockProfileCleanup != nil {
		blockProfileCleanup()
	}

	if memProfile != "" {
		f, err := os.Create(path.Clean(memProfile))
		if err != nil {
			log.Printf("error while creating mem-profile file: %v", err)
			return
		}

		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("error while closing mem-profile file: %v", err)
			}
		}()

		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Printf("error while writing heap profile: %v", err)
		}
	}
}

// Execute handles all invocations of the
// rosetta-cli cmd.
func Execute() error {
	defer rootPostRun()
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootFlags := rootCmd.PersistentFlags()
	rootFlags.StringVar(
		&configurationFile,
		"configuration-file",
		"",
		`Configuration file that provides connection and test settings.
If you would like to generate a starter configuration file (populated
with the defaults), run rosetta-cli configuration:create.

Any fields not populated in the configuration file will be populated with
default values.`,
	)
	rootFlags.StringVar(
		&cpuProfile,
		"cpu-profile",
		"",
		`Save the pprof cpu profile in the specified file`,
	)
	rootFlags.StringVar(
		&memProfile,
		"mem-profile",
		"",
		`Save the pprof mem profile in the specified file`,
	)
	rootFlags.StringVar(
		&blockProfile,
		"block-profile",
		"",
		`Save the pprof block profile in the specified file`,
	)
	rootCmd.AddCommand(versionCmd)

	// Configuration Commands
	rootCmd.AddCommand(configurationCreateCmd)
	rootCmd.AddCommand(configurationValidateCmd)

	// Check commands
	checkDataCmd.Flags().StringVar(
		&asserterConfigurationFile,
		"asserter-configuration-file",
		"", // Default to skip validation
		`Check that /network/options matches contents of file at this path`,
	)

	checkDataCmd.Flags().StringVar(
		&onlineURL,
		"online-url",
		"",
		"Override online node url in configuration file",
	)

	checkDataCmd.Flags().StringVar(
		&targetAccount,
		"target-account",
		"",
		"Override target account in configuration file",
	)

	checkDataCmd.Flags().Int64Var(
		&startIndex,
		"start-block",
		-1,
		`Start-block is the block height to start syncing from. This will override the start_index from configuration file`,
	)

	checkDataCmd.Flags().Int64Var(
		&endIndex,
		"end-block",
		-1,
		`End-block configures the syncer to stop once reaching a particular block height. This will override the index from configuration file`,
	)

	checkDataCmd.Flags().StringVar(
		&dataResultFile,
		"result-file",
		"",
		"Result-file configures the location of validation result. This will override the results_output_file from configuration file",
	)

	checkDataCmd.Flags().StringVar(
		&dataDirectory,
		"data-dir",
		"",
		"Data-dir configures the location of logs and data for validation. This will override the data_directory from configuration file",
	)

	checkDataCmd.Flags().Int64Var(
		&tableSize,
		"table-size",
		-1,
		"Table-size configures the TableSize for badger DB. If table-size != -1, this will override the table_size from configuration file",
	)

	checkDataCmd.Flags().BoolVar(
		&inMemoryMode,
		"in-memory-mode",
		false,
		"In-memory-mode configures badger DB inMeomry option. Only when in-memory-mode=true, this will override the all_in_memory_enabled",
	)

	checkDataCmd.Flags().StringVar(
		&requestUUID,
		"requestUUID",
		"",
		"requestUUID configures the requestUUID in logs, which aims to enable search logs by requestUUID",
	)

	checkDataCmd.Flags().StringVar(
		&InfoMetaData,
		"info-metadata",
		"",
		"metadata configures the metadata which aims to show in logs",
	)

	checkDataCmd.Flags().UintVar(
		&statusPort,
		"status-port",
		0,
		"status-port configures the status query port, this will override the status_port",
	)

	rootCmd.AddCommand(checkDataCmd)
	checkConstructionCmd.Flags().StringVar(
		&asserterConfigurationFile,
		"asserter-configuration-file",
		"", // Default to skip validation
		`Check that /network/options matches contents of file at this path`,
	)

	checkConstructionCmd.Flags().StringVar(
		&onlineURL,
		"online-url",
		"",
		"Override online node url in configuration file",
	)

	checkConstructionCmd.Flags().StringVar(
		&offlineURL,
		"offline-url",
		"",
		"Override offline node url in configuration file",
	)

	checkConstructionCmd.Flags().StringVar(
		&constructionResultFile,
		"result-file",
		"",
		"Result-file configures the location of validation result. This will override the results_output_file from configuration file",
	)

	checkConstructionCmd.Flags().StringVar(
		&requestUUID,
		"requestUUID",
		"",
		"requestUUID configures the requestUUID in logs, which aims to enable search logs by requestUUID",
	)

	checkConstructionCmd.Flags().UintVar(
		&statusPort,
		"status-port",
		0,
		"status-port configures the status query port, this will override the status_port",
	)

	rootCmd.AddCommand(checkConstructionCmd)

	// View Commands
	viewBlockCmd.Flags().BoolVar(
		&OnlyChanges,
		"only-changes",
		false,
		`Only print balance changes for accounts in the block`,
	)
	rootCmd.AddCommand(viewBlockCmd)
	rootCmd.AddCommand(viewAccountCmd)
	rootCmd.AddCommand(viewNetworksCmd)

	// Utils
	rootCmd.AddCommand(utilsAsserterConfigurationCmd)
	rootCmd.AddCommand(utilsTrainZstdCmd)

	// Benchmark commands
	rootCmd.AddCommand(checkPerfCmd)

	// check:spec
	checkSpecCmd.Flags().BoolVar(
		&checkAllSpecs,
		"all",
		false,
		`Verify both minimum and Coinbase spec requirements`,
	)
	rootCmd.AddCommand(checkSpecCmd)

	// Key Sign command
	rootCmd.AddCommand(keySignCmd)

	// Key Verify command
	rootCmd.AddCommand(keyVerifyCmd)

	keyGenCmd.Flags().StringVar(
		&curveType,
		"curve-type",
		string(types.Secp256k1),
		"curve type used to generate the public/private keypair",
	)
	// Key Gen command
	rootCmd.AddCommand(keyGenCmd)
}

func initConfig() {
	Context = context.Background()
	var err error

	// Use path provided by the environment variable if config path arg is not set.
	// Default configuration will be used if the env var is not
	if len(configurationFile) == 0 {
		configurationFile = os.Getenv(configEnvKey)
	}

	if len(configurationFile) == 0 {
		Config = configuration.DefaultConfiguration()
	} else {
		Config, err = configuration.LoadConfiguration(Context, configurationFile)
	}

	if err != nil {
		log.Fatalf("unable to load configuration: %s", err.Error())
	}

	// Override node url in configuration file when it's explicitly set via CLI
	if len(onlineURL) != 0 {
		Config.OnlineURL = onlineURL
	}
	if len(offlineURL) != 0 {
		Config.Construction.OfflineURL = offlineURL
	}

	if len(targetAccount) != 0 {
		Config.TargetAccount = targetAccount
	}

	// Override start and end syncing index in configuration file when it's explicitly set via CLI
	if startIndex != -1 {
		Config.Data.StartIndex = &startIndex
		// Configures rosetta-cli to lookup the balance of newly seen accounts at the
		// parent block before applying operations. Otherwise the balance will be 0.
		Config.Data.InitialBalanceFetchDisabled = false
	}

	if endIndex != -1 {
		Config.Data.EndConditions.Index = &endIndex
	}

	if len(dataResultFile) != 0 {
		Config.Data.ResultsOutputFile = dataResultFile
	}

	if len(constructionResultFile) != 0 {
		Config.Construction.ResultsOutputFile = constructionResultFile
	}

	if len(dataDirectory) != 0 {
		Config.DataDirectory = dataDirectory
	}

	if inMemoryMode {
		Config.AllInMemoryEnabled = inMemoryMode
	}

	if tableSize >= 2 && tableSize <= 100 {
		Config.TableSize = &tableSize
	} else if tableSize != -1 {
		log.Fatalf("table-size %d is not in the range [2, 100], please check your input", tableSize)
	}

	if len(requestUUID) != 0 {
		Config.RequestUUID = requestUUID
	}

	if statusPort > 0 {
		Config.Data.StatusPort = statusPort
		if Config.Construction != nil {
			Config.Construction.StatusPort = statusPort
		}
	}

	if len(InfoMetaData) != 0 {
		Config.InfoMetaData = InfoMetaData
	}
}

func ensureDataDirectoryExists() {
	// If data directory is not specified, we use a temporary directory
	// and delete its contents when execution is complete.
	if len(Config.DataDirectory) == 0 {
		tmpDir, err := utils.CreateTempDir()
		if err != nil {
			log.Fatalf("unable to create temporary directory: %s", err.Error())
		}

		Config.DataDirectory = tmpDir
	}
}

// handleSignals handles OS signals so we can ensure we close database
// correctly. We call multiple sigListeners because we
// may need to cancel more than 1 context.
func handleSignals(listeners *[]context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		color.Red("Received signal: %s", sig)
		SignalReceived = true
		for _, listener := range *listeners {
			listener()
		}
	}()
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print rosetta-cli version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.10.3")
	},
}
