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
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	viewAccountCmd = &cobra.Command{
		Use:   "view:account",
		Short: "View an account balance",
		Long: `While debugging, it is often useful to inspect the state
of an account at a certain block. This command allows you to look up
any account by providing a JSON representation of a types.AccountIdentifier
(and optionally a height to perform the query).

For example, you could run view:account '{"address":"interesting address"}' 1000
to lookup the balance of an interesting address at block 1000. Allowing the
address to specified as JSON allows for querying by SubAccountIdentifier.`,
		Run:  runViewAccountCmd,
		Args: cobra.MinimumNArgs(1),
	}
)

func runViewAccountCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	account := &types.AccountIdentifier{}
	if err := json.Unmarshal([]byte(args[0]), account); err != nil {
		log.Fatal(fmt.Errorf("%w: unable to unmarshal account %s", err, args[0]))
	}

	if err := asserter.AccountIdentifier(account); err != nil {
		log.Fatal(fmt.Errorf("%w: invalid account identifier %+v", err, account))
	}

	// Create a new fetcher
	newFetcher := fetcher.New(
		ServerURL,
	)

	// Initialize the fetcher's asserter
	primaryNetwork, _, err := newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Print the primary network and network status
	// TODO: support specifying which network to get block from
	log.Printf("Primary Network: %s\n", types.PrettyPrintStruct(primaryNetwork))

	var lookupBlock *types.PartialBlockIdentifier
	if len(args) > 1 {
		index, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			log.Fatal(fmt.Errorf("%w: unable to parse index %s", err, args[0]))
		}

		lookupBlock = &types.PartialBlockIdentifier{Index: &index}
	}

	block, amounts, metadata, err := newFetcher.AccountBalanceRetry(
		ctx,
		primaryNetwork,
		account,
		lookupBlock,
	)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to fetch account %+v", err, account))
	}

	log.Printf("Amounts: %s\n", types.PrettyPrintStruct(amounts))
	log.Printf("Metadata: %s\n", types.PrettyPrintStruct(metadata))
	log.Printf("Balance Fetched At: %s\n", types.PrettyPrintStruct(block))
}
