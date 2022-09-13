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
	"log"
	"time"

	"github.com/coinbase/rosetta-cli/pkg/errors"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	viewNetworksCmd = &cobra.Command{
		Use:   "view:networks",
		Short: "View all network statuses",
		Long: `While debugging a Data API implementation, it can be very
useful to view network(s) status. This command fetches the network
status from all available networks and prints it to the terminal.

If this command errors, it is likely because the /network/* endpoints are
not formatted correctly.`,
		RunE: runViewNetworksCmd,
	}
)

func runViewNetworksCmd(cmd *cobra.Command, args []string) error {
	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}
	if Config.ForceRetry {
		fetcherOpts = append(fetcherOpts, fetcher.WithForceRetry())
	}

	f := fetcher.New(
		Config.OnlineURL,
		fetcherOpts...,
	)

	// Attempt to fetch network list
	networkList, fetchErr := f.NetworkListRetry(Context, nil)
	if fetchErr != nil {
		return fmt.Errorf("unable to get network list: %w", fetchErr.Err)
	}

	if len(networkList.NetworkIdentifiers) == 0 {
		return errors.ErrNoAvailableNetwork
	}

	for _, network := range networkList.NetworkIdentifiers {
		color.Cyan(types.PrettyPrintStruct(network))
		networkOptions, fetchErr := f.NetworkOptions(
			Context,
			network,
			nil,
		)
		if fetchErr != nil {
			return fmt.Errorf("unable to get network options: %w", fetchErr.Err)
		}

		log.Printf("Network options: %s\n", types.PrettyPrintStruct(networkOptions))

		networkStatus, fetchErr := f.NetworkStatusRetry(
			Context,
			network,
			nil,
		)
		if fetchErr != nil {
			return fmt.Errorf("unable to get network status: %w", fetchErr.Err)
		}

		log.Printf("Network status: %s\n", types.PrettyPrintStruct(networkStatus))
	}

	return nil
}
