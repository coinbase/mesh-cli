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
	"log"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/spf13/cobra"
)

var (
	configurationValidateCmd = &cobra.Command{
		Use:   "configuration:validate",
		Short: "Ensure a configuration file at the provided path is formatted correctly",
		Run:   runConfigurationValidateCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runConfigurationValidateCmd(cmd *cobra.Command, args []string) {
	_, err := configuration.LoadConfiguration(args[0])
	if err != nil {
		log.Fatalf("%s: unable to save configuration file to %s", err.Error(), args[0])
	}

	log.Println("Configuration file validated!")
}
