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
	"sort"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	utilsAsserterConfigurationCmd = &cobra.Command{
		Use:   "utils:asserter-configuration",
		Short: "Generate a static configuration file for the Asserter",
		Long: `In production deployments, it is useful to initialize the response
Asserter (https://github.com/coinbase/rosetta-sdk-go/tree/master/asserter) using
a static configuration instead of initializing a configuration dynamically
from the node. This allows a client to error on new types/statuses that may
have been added in an update instead of silently erroring.

To use this command, simply provide an absolute path as the argument for where
the configuration file should be saved (in JSON).`,
		RunE: runCreateConfigurationCmd,
		Args: cobra.ExactArgs(1),
	}
)

func runCreateConfigurationCmd(cmd *cobra.Command, args []string) error {
	// Create a new fetcher
	newFetcher := fetcher.New(
		Config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	)

	// Initialize the fetcher's asserter
	_, _, fetchErr := newFetcher.InitializeAsserter(Context, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return fmt.Errorf("failed to initialize asserter for fetcher: %w", fetchErr.Err)
	}

	configuration, err := newFetcher.Asserter.ClientConfiguration()
	if err != nil {
		return fmt.Errorf("unable to generate asserter configuration: %w", err)
	}

	sortArrayFieldsOnConfiguration(configuration)

	if err := utils.SerializeAndWrite(args[0], configuration); err != nil {
		return fmt.Errorf("unable to serialize asserter configuration: %w", err)
	}

	color.Green("Configuration file saved!")
	return nil
}

func sortArrayFieldsOnConfiguration(configuration *asserter.Configuration) {
	sort.Strings(configuration.AllowedOperationTypes)
	sort.Slice(configuration.AllowedOperationStatuses, func(i, j int) bool {
		return configuration.AllowedOperationStatuses[i].Status < configuration.AllowedOperationStatuses[j].Status
	})
	sort.Slice(configuration.AllowedErrors, func(i, j int) bool {
		return configuration.AllowedErrors[i].Code < configuration.AllowedErrors[j].Code
	})
}
