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
	"time"

	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	viewBlockCmd = &cobra.Command{
		Use:   "view:block",
		Short: "View a block",
		Long: `While debugging a Data API implementation, it can be very
useful to inspect block contents. This command allows you to fetch any
block by index to inspect its contents. It uses the
fetcher (https://github.com/coinbase/rosetta-sdk-go/tree/master/fetcher) package
to automatically get all transactions in the block and assert the format
of the block is correct before printing.

If this command errors, it is likely because the block you are trying to
fetch is formatted incorrectly.`,
		Run:  runViewBlockCmd,
		Args: cobra.ExactArgs(1),
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
		Config.OnlineURL,
		fetcher.WithRetryElapsedTime(ExtendedRetryElapsedTime),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout)*time.Second),
	)

	// Initialize the fetcher's asserter
	//
	// Behind the scenes this makes a call to get the
	// network status and uses the response to inform
	// the asserter what are valid responses.
	_, _, err = newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	_, err = utils.CheckNetworkSupported(ctx, Config.Network, newFetcher)
	if err != nil {
		log.Fatalf("%s: unable to confirm network is supported", err.Error())
	}

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
		Config.Network,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to fetch block", err))
	}

	log.Printf("Current Block: %s\n", types.PrettyPrintStruct(block))

	// Print out all balance changes in a given block. This does NOT exempt
	// any operations/accounts from parsing.
	p := parser.New(newFetcher.Asserter, func(*types.Operation) bool { return false })
	changes, err := p.BalanceChanges(ctx, block, false)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to calculate balance changes", err))
	}

	log.Printf("Balance Changes: %s\n", types.PrettyPrintStruct(changes))

	// Print out all OperationGroups for each transaction in a block.
	for _, tx := range block.Transactions {
		log.Printf(
			"Transaction %s Operation Groups: %s\n",
			tx.TransactionIdentifier.Hash,
			types.PrettyPrintStruct(parser.GroupOperations(tx)),
		)
	}
}
