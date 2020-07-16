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
	"log"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	viewNetworkCmd = &cobra.Command{
		Use:   "view:network",
		Short: "View network status",
		Long: `While debugging a Data API implementation, it can be very
useful to view network(s) status. This command fetches the network
status from all available networks and prints it to the terminal.

If this command errors, it is likely because the /network/* endpoints are
not formatted correctly.`,
		Run: runViewNetworkCmd,
	}
)

func runViewNetworkCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	f := fetcher.New(ServerURL)

	// Attempt to fetch network list
	networkList, err := f.NetworkListRetry(ctx, nil)
	if err != nil {
		log.Fatalf("%s: unable to fetch network list", err.Error())
	}

	if len(networkList.NetworkIdentifiers) == 0 {
		log.Fatal("no networks available")
	}

	for _, network := range networkList.NetworkIdentifiers {
		color.Cyan(types.PrettyPrintStruct(network))
		networkOptions, err := f.NetworkOptions(
			ctx,
			network,
			nil,
		)
		if err != nil {
			log.Fatalf("%s: unable to get network options", err.Error())
		}

		log.Printf("Network options: %s\n", types.PrettyPrintStruct(networkOptions))

		networkStatus, err := f.NetworkStatusRetry(
			ctx,
			network,
			nil,
		)
		if err != nil {
			log.Fatalf("%s: unable to get network status", err.Error())
		}

		log.Printf("Network status: %s\n", types.PrettyPrintStruct(networkStatus))
	}
}
