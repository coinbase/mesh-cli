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
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:               "rosetta-cli",
		Short:             "CLI for the Rosetta API",
		PersistentPreRunE: rootPreRun,
	}

	configurationFile string
	cpuProfile        string
	memProfile        string
	blockProfile      string

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
)

// rootPreRun is executed before the root command runs and sets up cpu
// profiling.
//
// Bassed on https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
func rootPreRun(*cobra.Command, []string) error {
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			return fmt.Errorf("%w: unable to create CPU profile file", err)
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
		f, err := os.Create(blockProfile)
		if err != nil {
			return fmt.Errorf("%w: unable to create block profile file", err)
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
		f, err := os.Create(memProfile)
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
	rootCmd.AddCommand(checkDataCmd)
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
}

func initConfig() {
	Context = context.Background()
	var err error
	if len(configurationFile) == 0 {
		Config = configuration.DefaultConfiguration()
	} else {
		Config, err = configuration.LoadConfiguration(Context, configurationFile)
	}
	if err != nil {
		log.Fatalf("%s: unable to load configuration", err.Error())
	}
}

func ensureDataDirectoryExists() {
	// If data directory is not specified, we use a temporary directory
	// and delete its contents when execution is complete.
	if len(Config.DataDirectory) == 0 {
		tmpDir, err := utils.CreateTempDir()
		if err != nil {
			log.Fatalf("%s: unable to create temporary directory", err.Error())
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
		fmt.Println("v0.6.4")
	},
}
