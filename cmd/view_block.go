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
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	viewBlockCmd = &cobra.Command{
		Use:   "view:block",
		Short: "",
		Long:  ``,
		Run:   runViewBlockCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runViewBlockCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	index, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to parse index %s", err, args[0]))
	}

	// Create a new fetcher
	newFetcher := fetcher.New(
		ServerURL,
	)

	// Initialize the fetcher's asserter
	//
	// Behind the scenes this makes a call to get the
	// network status and uses the response to inform
	// the asserter what are valid responses.
	primaryNetwork, _, err := newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Print the primary network and network status
	// TODO: support specifying which network to get block from
	log.Printf("Primary Network: %s\n", types.PrettyPrintStruct(primaryNetwork))

	// Fetch the specified block with retries (automatically
	// asserted for correctness)
	//
	// On another note, notice that fetcher.BlockRetry
	// automatically fetches all transactions that are
	// returned in BlockResponse.OtherTransactions. If you use
	// the client directly, you will need to implement a mechanism
	// to fully populate the block by fetching all these
	// transactions.
	block, err := newFetcher.BlockRetry(
		ctx,
		primaryNetwork,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to fetch block", err))
	}

	log.Printf("Current Block: %s\n", types.PrettyPrintStruct(block))
}
