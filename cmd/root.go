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
	"fmt"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "rosetta-cli",
		Short: "CLI for the Rosetta API",
	}

	ConfigurationFile string
)

// Execute handles all invocations of the
// rosetta-cli cmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&ConfigurationFile,
		"configuration-file",
		"",
		"configuration file that provides connection and test settings",
		`If you would like to generate a starter configuration file (populated
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
	rootCmd.AddCommand(viewNetworkCmd)

	// Utils
	rootCmd.AddCommand(utilsAsserterConfigurationCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print rosetta-cli version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.3.2")
	},
}
