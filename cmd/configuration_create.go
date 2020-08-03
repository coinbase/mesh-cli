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
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/spf13/cobra"
)

var (
	configurationCreateCmd = &cobra.Command{
		Use:   "configuration:create",
		Short: "Create a default configuration file at the provided path",
		Run:   runConfigurationCreateCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runConfigurationCreateCmd(cmd *cobra.Command, args []string) {
	if err := utils.SerializeAndWrite(args[0], configuration.DefaultConfiguration()); err != nil {
		log.Fatalf("%s: unable to save configuration file to %s", err.Error(), args[0])
	}
}
