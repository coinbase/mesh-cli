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

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"
)

var (
	checkSpecCmd = &cobra.Command{
		Use:   "check:spec",
		Short: "Check that a Rosetta implementation satisfies Rosetta spec",
		Long: `Check:spec checks whether a Rosetta implementation satisfies either Coinbase-specific requirements or
minimum requirements specified in rosetta-api.org.

By default, check:spec will verify only Coinbase spec requirements. To verify the minimum requirements as well,
add the --all flag to the check:spec command:

rosetta-cli check:spec --all --configuration-file [filepath]
		
The minimum requirements verify whether an API response contains the required fields, and that the fields are 
correctly formatted with proper values. For example, it would check whether the response of /network/list
contains a list of network identifiers.
		
The Coinbase specific requirements are not documented in rosetta-api.org. However, we highly recommend that your
implementation satisfies them. This ensures that, when you want to integrate your asset into the Coinbase platform,
you can limit or eliminate implementation issues.
		
Here are a few examples of Coinbase spec requirements:
1. The network_identifier in Rosetta configuration should be static. Network upgrade shouldn't change its value.
2. When block_identifier is not specified, the call to /block endpoint should return the tip block.
3. The online_url and offline_url should be different.`,
		RunE: runCheckSpecCmd,
	}
)

type checkSpec struct {
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
}

func newCheckSpec(ctx context.Context) (*checkSpec, error) {
	if Config.Construction == nil {
		return nil, cliErrs.ErrConstructionConfigMissing
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
			fmt.Errorf("unable to initialize asserter for online fetcher: %w", fetchErr.Err),
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
	if checkAllSpecs {
		printInfo("validating /network/options ...\n")
		output := checkSpecOutput{
			api: networkOptions,
			validation: map[checkSpecRequirement]checkSpecStatus{
				version: {
					status: checkSpecSuccess,
				},
				allow: {
					status: checkSpecSuccess,
				},
				offlineMode: {
					status: checkSpecSuccess,
				},
			},
		}
		defer printInfo("/network/options validated\n")

		// NetworkOptionsRetry handles validation of /network/options response
		// This is an endpoint for offline mode
		_, err := cs.offlineFetcher.NetworkOptionsRetry(ctx, Config.Network, nil)
		if err != nil {
			printError("unable to fetch network options: %v\n", err.Err)
			markAllValidationsFailed(output)
			return output
		}

		return output
	}

	return checkSpecOutput{}
}

func (cs *checkSpec) networkList(ctx context.Context) checkSpecOutput {
	printInfo("validating /network/list ...\n")
	output := checkSpecOutput{
		api: networkList,
		validation: map[checkSpecRequirement]checkSpecStatus{
			staticNetworkID: {
				status:       checkSpecSuccess,
				coinbaseSpec: true,
			},
		},
	}

	if checkAllSpecs {
		output.validation[networkIDs] = checkSpecStatus{
			status: checkSpecSuccess,
		}
		output.validation[offlineMode] = checkSpecStatus{
			status: checkSpecSuccess,
		}
	}

	defer printInfo("/network/list validated\n")
	networks, err := cs.offlineFetcher.NetworkListRetry(ctx, nil)

	// endpoint for offline mode
	if err != nil {
		printError("unable to fetch network list: %v\n", err.Err)
		markAllValidationsFailed(output)
		return output
	}

	if checkAllSpecs && len(networks.NetworkIdentifiers) == 0 {
		printError("network_identifiers is required")
		setValidationStatusFailed(output, networkIDs)
	}

	// static network ID
	for _, network := range networks.NetworkIdentifiers {
		if isEqual(network.Network, Config.Network.Network) &&
			isEqual(network.Blockchain, Config.Network.Blockchain) {
			return output
		}
	}

	printError("network_identifier in configuration file is not returned by /network/list")
	setValidationStatusFailed(output, staticNetworkID)
	return output
}

func (cs *checkSpec) accountCoins(ctx context.Context) checkSpecOutput {
	if checkAllSpecs {
		printInfo("validating /account/coins ...\n")
		output := checkSpecOutput{
			api: accountCoins,
			validation: map[checkSpecRequirement]checkSpecStatus{
				blockID: {
					status: checkSpecSuccess,
				},
				coins: {
					status: checkSpecSuccess,
				},
			},
		}
		defer printInfo("/account/coins validated\n")

		if isUTXO() {
			acct, _, currencies, err := cs.getAccount(ctx)
			if err != nil {
				printError("unable to get an account: %v\n", err)
				markAllValidationsFailed(output)
				return output
			}
			if acct == nil {
				printError("%v\n", cliErrs.ErrAccountNullPointer)
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
				printError("unable to get coins for account %s: %v\n", types.PrintStruct(acct), fetchErr.Err)
				markAllValidationsFailed(output)
				return output
			}
		}

		return output
	}

	return checkSpecOutput{}
}

func (cs *checkSpec) block(ctx context.Context) checkSpecOutput {
	printInfo("validating /block ...\n")
	output := checkSpecOutput{
		api: block,
		validation: map[checkSpecRequirement]checkSpecStatus{
			defaultTip: {
				status:       checkSpecSuccess,
				coinbaseSpec: true,
			},
		},
	}
	defer printInfo("/block validated\n")

	if checkAllSpecs {
		output.validation[idempotent] = checkSpecStatus{
			status: checkSpecSuccess,
		}
	}

	res, fetchErr := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if fetchErr != nil {
		printError("unable to get network status: %v\n", fetchErr.Err)
		markAllValidationsFailed(output)
		return output
	}

	if checkAllSpecs {
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
				printError("unable to fetch block %s: %v\n", types.PrintStruct(blockID), fetchErr.Err)
				markAllValidationsFailed(output)
				return output
			}

			if block == nil {
				block = b
			} else if !isEqual(types.Hash(*block), types.Hash(*b)) {
				printError("%v\n", cliErrs.ErrBlockNotIdempotent)
				setValidationStatusFailed(output, idempotent)
			}
		}
	}

	// fetch the tip block again
	res, fetchErr = cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if fetchErr != nil {
		printError("unable to get network status: %v\n", fetchErr.Err)
		setValidationStatusFailed(output, defaultTip)
		return output
	}
	tip := res.CurrentBlockIdentifier

	// tip should be returned if block_identifier is not specified
	emptyBlockID := &types.PartialBlockIdentifier{}
	block, fetchErr := cs.onlineFetcher.BlockRetry(ctx, Config.Network, emptyBlockID)
	if fetchErr != nil {
		printError("unable to fetch tip block: %v\n", fetchErr.Err)
		setValidationStatusFailed(output, defaultTip)
		return output
	}

	// block index returned from /block should be >= the index returned by /network/status
	if isNegative(block.BlockIdentifier.Index - tip.Index) {
		printError("%v\n", cliErrs.ErrBlockTip)
		setValidationStatusFailed(output, defaultTip)
	}

	return output
}

func (cs *checkSpec) errorObject(ctx context.Context) checkSpecOutput {
	if checkAllSpecs {
		printInfo("validating error object ...\n")
		output := checkSpecOutput{
			api: errorObject,
			validation: map[checkSpecRequirement]checkSpecStatus{
				errorCode: {
					status: checkSpecSuccess,
				},
				errorMessage: {
					status: checkSpecSuccess,
				},
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

	return checkSpecOutput{}
}

// Searching for an account backwards from the tip
func (cs *checkSpec) getAccount(ctx context.Context) (
	*types.AccountIdentifier,
	*types.PartialBlockIdentifier,
	[]*types.Currency,
	error) {
	res, err := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get network status of network %s: %w", types.PrintStruct(Config.Network), err.Err)
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
			return nil, nil, nil, fmt.Errorf("unable to fetch block at index %d: %w", i, err.Err)
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
		return fmt.Errorf("unable to create checkSpec object with online URL: %w", err)
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
