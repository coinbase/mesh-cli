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
	"syscall"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	// ExtendedRetryElapsedTime is used to override the default fetcher
	// retry elapsed time. In practice, extending the retry elapsed time
	// has prevented retry exhaustion errors when many goroutines are
	// used to fetch data from the Rosetta server.
	//
	// TODO: make configurable
	ExtendedRetryElapsedTime = 5 * time.Minute
)

var (
	rootCmd = &cobra.Command{
		Use:   "rosetta-cli",
		Short: "CLI for the Rosetta API",
	}

	configurationFile string

	// Config is the populated *configuration.Configuration from
	// the configurationFile. If none is provided, this is set
	// to the default settings.
	Config *configuration.Configuration

	// SignalReceived is set to true when a signal causes us to exit. This makes
	// determining the error message to show on exit much more easy.
	SignalReceived = false
)

// Execute handles all invocations of the
// rosetta-cli cmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(
		&configurationFile,
		"configuration-file",
		"",
		`Configuration file that provides connection and test settings.
If you would like to generate a starter configuration file (populated
with the defaults), run rosetta-cli configuration:create.

Any fields not populated in the configuration file will be populated with
default values.`,
	)
	rootCmd.AddCommand(versionCmd)

	// Configuration Commands
	rootCmd.AddCommand(configurationCreateCmd)
	rootCmd.AddCommand(configurationValidateCmd)

	// Check commands
	rootCmd.AddCommand(checkDataCmd)
	rootCmd.AddCommand(checkConstructionCmd)

	// View Commands
	rootCmd.AddCommand(viewBlockCmd)
	rootCmd.AddCommand(viewAccountCmd)
	rootCmd.AddCommand(viewNetworksCmd)

	// Utils
	rootCmd.AddCommand(utilsAsserterConfigurationCmd)
}

func initConfig() {
	var err error
	if len(configurationFile) == 0 {
		Config = configuration.DefaultConfiguration()
	} else {
		Config, err = configuration.LoadConfiguration(configurationFile)
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
func handleSignals(listeners []context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		color.Red("Received signal: %s", sig)
		SignalReceived = true
		for _, listener := range listeners {
			listener()
		}
	}()
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print rosetta-cli version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.4.0")
	},
}
