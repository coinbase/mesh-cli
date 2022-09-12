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
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/spf13/cobra"
)

var (
	viewAccountCmd = &cobra.Command{
		Use:   "view:balance",
		Short: "View an account balance",
		Long: `While debugging, it is often useful to inspect the state
of an account at a certain block. This command allows you to look up
any account by providing a JSON representation of a types.AccountIdentifier
(and optionally a height to perform the query).

For example, you could run view:balance '{"address":"interesting address"}' 1000
to lookup the balance of an interesting address at block 1000. Allowing the
address to specified as JSON allows for querying by SubAccountIdentifier.`,
		RunE: runViewBalanceCmd,
		Args: cobra.MinimumNArgs(1),
	}
)

func runViewBalanceCmd(cmd *cobra.Command, args []string) error {
	account := &types.AccountIdentifier{}
	if err := json.Unmarshal([]byte(args[0]), account); err != nil {
		return fmt.Errorf("unable to unmarshal account %s: %w", args[0], err)
	}

	if err := asserter.AccountIdentifier(account); err != nil {
		return fmt.Errorf("invalid account identifier %s: %w", types.PrintStruct(account), err)
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
	_, _, fetchErr := newFetcher.InitializeAsserter(Context, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return fmt.Errorf("unable to initialize asserter for fetcher: %w", fetchErr.Err)
	}

	_, err := utils.CheckNetworkSupported(Context, Config.Network, newFetcher)
	if err != nil {
		return fmt.Errorf("unable to confirm network %s is supported: %w", types.PrintStruct(Config.Network), err)
	}

	var lookupBlock *types.PartialBlockIdentifier
	if len(args) > 1 {
		index, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse index %s: %w", args[0], err)
		}

		lookupBlock = &types.PartialBlockIdentifier{Index: &index}
	}

	block, amounts, metadata, fetchErr := newFetcher.AccountBalanceRetry(
		Context,
		Config.Network,
		account,
		lookupBlock,
		nil,
	)
	if fetchErr != nil {
		return fmt.Errorf("unable to fetch account balance for account %s: %w", types.PrintStruct(account), fetchErr.Err)
	}

	log.Printf("Amounts: %s\n", types.PrettyPrintStruct(amounts))
	log.Printf("Metadata: %s\n", types.PrettyPrintStruct(metadata))
	log.Printf("Balance Fetched At: %s\n", types.PrettyPrintStruct(block))

	return nil
}
