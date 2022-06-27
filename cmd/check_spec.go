// Copyright 2022 Coinbase, Inc.
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
	"time"

	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	checkSpecCmd = &cobra.Command{
		Use:   "check:spec",
		Short: "Check a Rosetta implementation satisfies Rosetta spec",
		Long: `Detailed Rosetta spec can be found in https://www.rosetta-api.org/docs/Reference.html.
			Specifically, check:spec will examine the response from all data and construction API endpoints,
			and verifiy they have required fields and the values are properly populated and formatted.`,
		RunE: runCheckSpecCmd,
	}
)

type checkSpec struct {
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
}

func newCheckSpec(ctx context.Context) (*checkSpec, error) {
	if Config.Construction == nil {
		return nil, fmt.Errorf("%v", errRosettaConfigNoConstruction)
	}

	onlineFetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}

	offlineFetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.Construction.MaxOfflineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}

	if Config.ForceRetry {
		onlineFetcherOpts = append(onlineFetcherOpts, fetcher.WithForceRetry())
		offlineFetcherOpts = append(offlineFetcherOpts, fetcher.WithForceRetry())
	}

	onlineFetcher := fetcher.New(
		Config.OnlineURL,
		onlineFetcherOpts...,
	)
	offlineFetcher := fetcher.New(
		Config.Construction.OfflineURL,
		offlineFetcherOpts...,
	)

	_, _, fetchErr := onlineFetcher.InitializeAsserter(ctx, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return nil, results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%v: unable to initialize asserter for online node fetcher", fetchErr.Err),
			"",
			"",
		)
	}

	return &checkSpec{
		onlineFetcher:  onlineFetcher,
		offlineFetcher: offlineFetcher,
	}, nil
}

func (cs *checkSpec) networkOptions(ctx context.Context) checkSpecOutput {
	printInfo("validating /network/options ...\n")
	output := checkSpecOutput{
		api: networkOptions,
		validation: map[checkSpecRequirement]checkSpecStatus{
			version:     checkSpecSuccess,
			allow:       checkSpecSuccess,
			offlineMode: checkSpecSuccess,
		},
	}
	defer printInfo("/network/options validated\n")

	// NetworkOptionsRetry handles validation of /network/options response
	// This is an endpoint for offline mode
	_, err := cs.offlineFetcher.NetworkOptionsRetry(ctx, Config.Network, nil)
	if err != nil {
		printError("%v: unable to fetch network options\n", err.Err)
		markAllValidationsFailed(output)
		return output
	}

	return output
}

func (cs *checkSpec) networkList(ctx context.Context) checkSpecOutput {
	printInfo("validating /network/list ...\n")
	output := checkSpecOutput{
		api: networkList,
		validation: map[checkSpecRequirement]checkSpecStatus{
			networkIDs:      checkSpecSuccess,
			offlineMode:     checkSpecSuccess,
			staticNetworkID: checkSpecSuccess,
		},
	}
	defer printInfo("/network/list validated\n")

	// endpoint for offline mode
	networks, err := cs.offlineFetcher.NetworkListRetry(ctx, nil)
	if err != nil {
		printError("%v: unable to fetch network list", err.Err)
		markAllValidationsFailed(output)
		return output
	}

	if len(networks.NetworkIdentifiers) == 0 {
		printError("network_identifiers is required")
		setValidationStatusFailed(output, networkIDs)
	}

	for _, network := range networks.NetworkIdentifiers {
		if isEqual(network.Network, Config.Network.Network) &&
			isEqual(network.Blockchain, Config.Network.Blockchain) {
			return output
		}
	}

	// static network ID
	printError("network_identifier in configuration file is not returned by /network/list")
	setValidationStatusFailed(output, staticNetworkID)
	return output
}

func (cs *checkSpec) accountCoins(ctx context.Context) checkSpecOutput {
	printInfo("validating /account/coins ...\n")
	output := checkSpecOutput{
		api: accountCoins,
		validation: map[checkSpecRequirement]checkSpecStatus{
			blockID: checkSpecSuccess,
			coins:   checkSpecSuccess,
		},
	}
	defer printInfo("/account/coins validated\n")

	if isUTXO() {
		acct, _, currencies, err := cs.getAccount(ctx)
		if err != nil {
			printError("%v: unable to get an account\n", err)
			markAllValidationsFailed(output)
			return output
		}
		if err != nil {
			printError("%v\n", errAccountNullPointer)
			markAllValidationsFailed(output)
			return output
		}

		_, _, _, fetchErr := cs.onlineFetcher.AccountCoinsRetry(
			ctx,
			Config.Network,
			acct,
			false,
			currencies)
		if fetchErr != nil {
			printError("%v: unable to get coins for account: %v\n", fetchErr.Err, *acct)
			markAllValidationsFailed(output)
			return output
		}
	}

	return output
}

func (cs *checkSpec) block(ctx context.Context) checkSpecOutput {
	printInfo("validating /block ...\n")
	output := checkSpecOutput{
		api: block,
		validation: map[checkSpecRequirement]checkSpecStatus{
			idempotent: checkSpecSuccess,
			defaultTip: checkSpecSuccess,
		},
	}
	defer printInfo("/block validated\n")

	res, fetchErr := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if fetchErr != nil {
		printError("%v: unable to get network status\n", fetchErr.Err)
		markAllValidationsFailed(output)
		return output
	}

	// multiple calls with the same hash should return the same block
	var block *types.Block
	tip := res.CurrentBlockIdentifier
	callTimes := 3

	for i := 0; i < callTimes; i++ {
		blockID := types.PartialBlockIdentifier{
			Hash: &tip.Hash,
		}
		b, fetchErr := cs.onlineFetcher.BlockRetry(ctx, Config.Network, &blockID)
		if fetchErr != nil {
			printError("%v: unable to fetch block %v\n", fetchErr.Err, blockID)
			markAllValidationsFailed(output)
			return output
		}

		if block == nil {
			block = b
		} else if !isEqual(types.Hash(*block), types.Hash(*b)) {
			printError("%v\n", errBlockNotIdempotent)
			setValidationStatusFailed(output, idempotent)
		}
	}

	// fetch the tip block again
	res, fetchErr = cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if fetchErr != nil {
		printError("%v: unable to get network status\n", fetchErr.Err)
		setValidationStatusFailed(output, defaultTip)
		return output
	}
	tip = res.CurrentBlockIdentifier

	// tip shoud be returned if block_identifier is not specified
	emptyBlockID := &types.PartialBlockIdentifier{}
	block, fetchErr = cs.onlineFetcher.BlockRetry(ctx, Config.Network, emptyBlockID)
	if fetchErr != nil {
		printError("%v: unable to fetch tip block\n", fetchErr.Err)
		setValidationStatusFailed(output, defaultTip)
		return output
	}

	// block index returned from /block should be >= the index returned by /network/status
	if isNegative(block.BlockIdentifier.Index - tip.Index) {
		printError("%v\n", errBlockTip)
		setValidationStatusFailed(output, defaultTip)
	}

	return output
}

func (cs *checkSpec) errorObject(ctx context.Context) checkSpecOutput {
	printInfo("validating error object ...\n")
	output := checkSpecOutput{
		api: errorObject,
		validation: map[checkSpecRequirement]checkSpecStatus{
			errorCode:    checkSpecSuccess,
			errorMessage: checkSpecSuccess,
		},
	}
	defer printInfo("error object validated\n")

	printInfo("%v\n", "sending request to /network/status ...")
	emptyNetwork := &types.NetworkIdentifier{}
	_, err := cs.onlineFetcher.NetworkStatusRetry(ctx, emptyNetwork, nil)
	validateErrorObject(err, output)

	printInfo("%v\n", "sending request to /network/options ...")
	_, err = cs.onlineFetcher.NetworkOptionsRetry(ctx, emptyNetwork, nil)
	validateErrorObject(err, output)

	printInfo("%v\n", "sending request to /account/balance ...")
	emptyAcct := &types.AccountIdentifier{}
	emptyPartBlock := &types.PartialBlockIdentifier{}
	emptyCur := []*types.Currency{}
	_, _, _, err = cs.onlineFetcher.AccountBalanceRetry(ctx, emptyNetwork, emptyAcct, emptyPartBlock, emptyCur)
	validateErrorObject(err, output)

	if isUTXO() {
		printInfo("%v\n", "sending request to /account/coins ...")
		_, _, _, err = cs.onlineFetcher.AccountCoinsRetry(ctx, emptyNetwork, emptyAcct, false, emptyCur)
		validateErrorObject(err, output)
	} else {
		printInfo("%v\n", "skip /account/coins for account based chain")
	}

	printInfo("%v\n", "sending request to /block ...")
	_, err = cs.onlineFetcher.BlockRetry(ctx, emptyNetwork, emptyPartBlock)
	validateErrorObject(err, output)

	printInfo("%v\n", "sending request to /block/transaction ...")
	emptyTx := []*types.TransactionIdentifier{}
	emptyBlock := &types.BlockIdentifier{}
	_, err = cs.onlineFetcher.UnsafeTransactions(ctx, emptyNetwork, emptyBlock, emptyTx)
	validateErrorObject(err, output)

	return output
}

// Searching for an account backwards from the tip
func (cs *checkSpec) getAccount(ctx context.Context) (
	*types.AccountIdentifier,
	*types.PartialBlockIdentifier,
	[]*types.Currency,
	error) {
	res, err := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%v: unable to get network status", err.Err)
	}

	var acct *types.AccountIdentifier
	var blockID *types.PartialBlockIdentifier
	tip := res.CurrentBlockIdentifier.Index
	genesis := res.GenesisBlockIdentifier.Index
	currencies := []*types.Currency{}

	for i := tip; i >= genesis && acct == nil; i-- {
		blockID = &types.PartialBlockIdentifier{
			Index: &i,
		}

		block, err := cs.onlineFetcher.BlockRetry(ctx, Config.Network, blockID)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%v: unable to fetch block at index: %v", err.Err, i)
		}

		// looking for an account in block transactions
		for _, tx := range block.Transactions {
			for _, op := range tx.Operations {
				if op.Account != nil && op.Amount.Currency != nil {
					acct = op.Account
					currencies = append(currencies, op.Amount.Currency)
					break
				}
			}

			if acct != nil {
				break
			}
		}
	}

	return acct, blockID, currencies, nil
}

func runCheckSpecCmd(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	cs, err := newCheckSpec(ctx)
	if err != nil {
		return fmt.Errorf("%v: unable to create checkSpec object with online URL", err)
	}

	output := []checkSpecOutput{}
	// validate api endpoints
	output = append(output, cs.networkList(ctx))
	output = append(output, cs.networkOptions(ctx))
	output = append(output, cs.accountCoins(ctx))
	output = append(output, cs.block(ctx))
	output = append(output, cs.errorObject(ctx))
	output = append(output, twoModes())

	printInfo("check:spec is complete\n")
	printCheckSpecOutputHeader()
	for _, o := range output {
		printCheckSpecOutputBody(o)
	}

	return nil
}
