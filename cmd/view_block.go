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
	"strconv"
	"time"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
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
		RunE: runViewBlockCmd,
		Args: cobra.ExactArgs(1),
	}
)

func printChanges(balanceChanges []*parser.BalanceChange) error {
	for _, balanceChange := range balanceChanges {
		parsedDiff, err := types.BigInt(balanceChange.Difference)
		if err != nil {
			return fmt.Errorf("unable to parse balance change difference: %w", err)
		}

		if parsedDiff.Sign() == 0 {
			continue
		}

		fmt.Println(
			types.PrintStruct(balanceChange.Account),
			"->",
			utils.PrettyAmount(parsedDiff, balanceChange.Currency),
		)
	}

	return nil
}

func runViewBlockCmd(_ *cobra.Command, args []string) error {
	index, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse index %s: %w", args[0], err)
	}

	// Create a new fetcher
	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}
	if Config.ForceRetry {
		fetcherOpts = append(fetcherOpts, fetcher.WithForceRetry())
	}

	newFetcher := fetcher.New(
		Config.OnlineURL,
		fetcherOpts...,
	)

	// Initialize the fetcher's asserter
	//
	// Behind the scenes this makes a call to get the
	// network status and uses the response to inform
	// the asserter what are valid responses.
	_, _, fetchErr := newFetcher.InitializeAsserter(Context, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return fmt.Errorf("unable to initialize asserter for fetcher: %w", fetchErr.Err)
	}

	_, err = utils.CheckNetworkSupported(Context, Config.Network, newFetcher)
	if err != nil {
		return fmt.Errorf("unable to confirm network %s is supported: %w", types.PrintStruct(Config.Network), err)
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
	block, fetchErr := newFetcher.BlockRetry(
		Context,
		Config.Network,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)
	if fetchErr != nil {
		return fmt.Errorf("unable to fetch block %d: %w", index, fetchErr.Err)
	}
	// It's valid for a block to be omitted without triggering an error
	if block == nil {
		return cliErrs.ErrBlockNotFound
	}

	fmt.Printf("\n")
	if !OnlyChanges {
		color.Cyan("Current Block:")
		fmt.Println(types.PrettyPrintStruct(block))
	}

	// Print out all balance changes in a given block. This does NOT exempt
	// any operations/accounts from parsing.
	color.Cyan("Balance Changes:")
	p := parser.New(newFetcher.Asserter, func(*types.Operation) bool { return false }, nil)
	balanceChanges, err := p.BalanceChanges(Context, block, false)
	if err != nil {
		return fmt.Errorf("unable to calculate balance changes: %w", err)
	}

	fmt.Println("Cumulative:", block.BlockIdentifier.Hash)

	if err := printChanges(balanceChanges); err != nil {
		return err
	}

	fmt.Printf("\n")

	// Print out balance changes by transaction hash
	//
	// TODO: modify parser to allow for calculating balance
	// changes for a single transaction.
	for _, tx := range block.Transactions {
		balanceChanges, err := p.BalanceChanges(Context, &types.Block{
			Transactions: []*types.Transaction{
				tx,
			},
		}, false)
		if err != nil {
			return fmt.Errorf("unable to calculate balance changes: %w", err)
		}

		fmt.Println("Transaction:", tx.TransactionIdentifier.Hash)

		if err := printChanges(balanceChanges); err != nil {
			return err
		}
		fmt.Printf("\n")
	}

	if !OnlyChanges {
		// Print out all OperationGroups for each transaction in a block.
		color.Cyan("Operation Groups:")
		for _, tx := range block.Transactions {
			fmt.Printf(
				"Transaction %s Operation Groups: %s\n",
				tx.TransactionIdentifier.Hash,
				types.PrettyPrintStruct(parser.GroupOperations(tx)),
			)
		}
	}

	return nil
}
