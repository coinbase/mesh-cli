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

	// ServerURL is the base URL for a Rosetta
	// server to validate.
	ServerURL string
)

// Execute handles all invocations of the
// rosetta-cli cmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&ServerURL,
		"server-url",
		"http://localhost:8080",
		"base URL for a Rosetta server",
	)

	rootCmd.AddCommand(checkCmd)
	rootCmd.AddCommand(viewBlockCmd)
	rootCmd.AddCommand(viewAccountCmd)
	rootCmd.AddCommand(createConfigurationCmd)
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print rosetta-cli version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.2.5")
	},
}
