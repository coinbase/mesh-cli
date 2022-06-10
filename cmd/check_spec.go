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
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/tidwall/sjson"
	"log"
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
	onlineFetcher     *fetcher.Fetcher
	offlineFetcher    *fetcher.Fetcher
	coordinatorHelper *processor.CoordinatorHelper
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

	_, _, fetchErr = offlineFetcher.InitializeAsserter(ctx, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return nil, results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%w: unable to initialize asserter for offline node fetcher", fetchErr.Err),
			"",
			"",
		)
	}

	coordinatorHelper := processor.NewCoordinatorHelper(offlineFetcher, onlineFetcher, nil, nil, nil, nil, nil, nil, nil, nil, Config.Construction.Quiet)

	return &checkSpec{
		onlineFetcher:     onlineFetcher,
		offlineFetcher:    offlineFetcher,
		coordinatorHelper: coordinatorHelper,
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

	res, err := cs.offlineFetcher.NetworkOptionsRetry(ctx, Config.Network, nil)
	if err != nil {
		printError("%v: unable to fetch network options\n", err.Err)
		markAllValidationsStatus(output, checkSpecFailure)
		return output
	}

	// version is required
	if res.Version == nil {
		setValidationStatus(output, version, checkSpecFailure)
		printError("%v: unable to find version in /network/options response\n", errVersionNullPointer)
	}

	if err := validateVersion(res.Version.RosettaVersion); err != nil {
		setValidationStatus(output, version, checkSpecFailure)
		printError("%v\n", err)
	}

	if err := validateVersion(res.Version.NodeVersion); err != nil {
		setValidationStatus(output, version, checkSpecFailure)
		printError("%v\n", err)
	}

	// allow is required
	if res.Allow == nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v: unable to find allow in /network/options response\n", errAllowNullPointer)
	}

	if err := validateOperationStatuses(res.Allow.OperationStatuses); err != nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v\n", err)
	}

	if err := validateOperationTypes(res.Allow.OperationTypes); err != nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v\n", err)
	}

	if err := validateErrors(res.Allow.Errors); err != nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v\n", err)
	}

	if err := validateCallMethods(res.Allow.CallMethods); err != nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v\n", err)
	}

	if err := validateBalanceExemptions(res.Allow.BalanceExemptions); err != nil {
		setValidationStatus(output, allow, checkSpecFailure)
		printError("%v\n", err)
	}

	return output
}

func (cs *checkSpec) networkStatus(ctx context.Context) checkSpecOutput {
	printInfo("validating /network/status ...\n")
	output := checkSpecOutput{
		api: networkStatus,
		validation: map[checkSpecRequirement]checkSpecStatus{
			currentBlockID:   checkSpecSuccess,
			currentBlockTime: checkSpecSuccess,
			genesisBlockID:   checkSpecSuccess,
		},
	}
	defer printInfo("/network/status validated\n")

	res, err := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if err != nil {
		printError("%v: unable to fetch network status\n", err.Err)
		markAllValidationsStatus(output, checkSpecFailure)
		return output
	}

	// current_block_identifier is required
	if err := validateBlockIdentifier(res.CurrentBlockIdentifier); err != nil {
		printError("%v\n", err)
		setValidationStatus(output, currentBlockID, checkSpecFailure)
	}

	// current_block_timestamp is required
	if err := validateTimestamp(res.CurrentBlockTimestamp); err != nil {
		printError("%v\n", err)
		setValidationStatus(output, currentBlockTime, checkSpecFailure)
	}

	// genesis_block_identifier is required
	if err := validateBlockIdentifier(res.GenesisBlockIdentifier); err != nil {
		printError("%v\n", err)
		setValidationStatus(output, genesisBlockID, checkSpecFailure)
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

	networks, err := cs.offlineFetcher.NetworkList(ctx, nil)
	if err != nil {
		printError("%v: unable to fetch network list", err.Err)
		markAllValidationsStatus(output, checkSpecFailure)
		return output
	}

	if len(networks.NetworkIdentifiers) == 0 {
		printError("network_identifiers is required")
		setValidationStatus(output, networkIDs, checkSpecFailure)
	}

	for _, network := range networks.NetworkIdentifiers {
		if isEqual(network.Network, Config.Network.Network) &&
			isEqual(network.Blockchain, Config.Network.Blockchain) {
			return output
		}
	}

	printError("network_identifier in configuration file is not returned by /network/list")
	setValidationStatus(output, staticNetworkID, checkSpecFailure)
	return output
}

func (cs *checkSpec) accountBalance(ctx context.Context) checkSpecOutput {
	printInfo("validating /account/balance ...\n")
	output := checkSpecOutput{
		api: accountBalance,
		validation: map[checkSpecRequirement]checkSpecStatus{
			blockID:  checkSpecSuccess,
			balances: checkSpecSuccess,
		},
	}
	defer printInfo("/account/balance validated\n")

	acct, partBlockID, currencies, err := cs.getAccount(ctx)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		printError("%v: unable to get an account\n", err)
		return output
	}
	if acct == nil {
		markAllValidationsStatus(output, checkSpecFailure)
		printError("%v\n", errAccountNullPointer)
		return output
	}

	// fetch account balance
	block, amt, _, fetchErr := cs.onlineFetcher.AccountBalanceRetry(
		ctx,
		Config.Network,
		acct,
		partBlockID,
		currencies)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		printError("%v: unable to fetch balance for account: %v\n", fetchErr.Err, *acct)
		return output
	}

	// block_identifier is required
	if err := validateBlockIdentifier(block); err != nil {
		printError("%v\n", err)
		setValidationStatus(output, blockID, checkSpecFailure)
	}

	// balances is required
	if err := validateBalances(amt); err != nil {
		printError("%v\n", err)
		setValidationStatus(output, balances, checkSpecFailure)
	}

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
			markAllValidationsStatus(output, checkSpecFailure)
			return output
		}
		if err != nil {
			printError("%v\n", errAccountNullPointer)
			markAllValidationsStatus(output, checkSpecFailure)
			return output
		}

		block, cs, _, fetchErr := cs.onlineFetcher.AccountCoinsRetry(
			ctx,
			Config.Network,
			acct,
			false,
			currencies)
		if fetchErr != nil {
			printError("%v: unable to get coins for account: %v\n", fetchErr.Err, *acct)
			markAllValidationsStatus(output, checkSpecFailure)
			return output
		}

		// block_identifier is required
		err = validateBlockIdentifier(block)
		if err != nil {
			printError("%v\n", err)
			setValidationStatus(output, blockID, checkSpecFailure)
		}

		// coins is required
		err = validateCoins(cs)
		if err != nil {
			printError("%v\n", err)
			setValidationStatus(output, coins, checkSpecFailure)
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
		markAllValidationsStatus(output, checkSpecFailure)
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
			markAllValidationsStatus(output, checkSpecFailure)
			return output
		}

		if block == nil {
			block = b
		} else if !isEqual(types.Hash(*block), types.Hash(*b)) {
			printError("%v\n", errBlockNotIdempotent)
			setValidationStatus(output, idempotent, checkSpecFailure)
		}
	}

	// fetch the tip block again
	res, fetchErr = cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if fetchErr != nil {
		printError("%v: unable to get network status\n", fetchErr.Err)
		setValidationStatus(output, defaultTip, checkSpecFailure)
		return output
	}
	tip = res.CurrentBlockIdentifier

	// tip shoud be returned if block_identifier is not specified
	emptyBlockID := &types.PartialBlockIdentifier{}
	block, fetchErr = cs.onlineFetcher.BlockRetry(ctx, Config.Network, emptyBlockID)
	if fetchErr != nil {
		printError("%v: unable to fetch tip block\n", fetchErr.Err)
		setValidationStatus(output, defaultTip, checkSpecFailure)
		return output
	}

	// block index returned from /block should be >= the index returned by /network/status
	if isNegative(block.BlockIdentifier.Index - tip.Index) {
		printError("%v\n", errBlockTip)
		setValidationStatus(output, defaultTip, checkSpecFailure)
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

func (cs *checkSpec) ConstructionPreprocess(ctx context.Context, broadcast *job.Broadcast) (map[string]interface{}, error) {
	printInfo("validating /construction/preprocess ...\n")
	output := checkSpecConstructionOutput[constructionPreprocess]
	defer printInfo("/construction/preprocess validated\n")

	metadata, _, err := cs.coordinatorHelper.Preprocess(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		broadcast.Metadata,
	)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return nil, fmt.Errorf("%w: unable to send preprocess request \n", err)
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return metadata, nil
}

func (cs *checkSpec) ConstructionMetadata(
	ctx context.Context,
	broadcast *job.Broadcast,
	metadata map[string]interface{},
	publicKeys []*types.PublicKey,
) (map[string]interface{}, error) {
	printInfo("validating /construction/metadata ...\n")
	output := checkSpecConstructionOutput[constructionMetadata]
	defer printInfo("/construction/metadata validated\n")

	// TODO: fill pk
	metadata, _, err := cs.coordinatorHelper.Metadata(ctx, broadcast.Network, metadata, nil)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return nil, fmt.Errorf("%w: unable to send metadata request \n", err)
	}
	if metadata == nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return nil, fmt.Errorf("metadata is required \n")
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return metadata, nil
}

func (cs *checkSpec) ConstructionPayloads(ctx context.Context, broadcast *job.Broadcast, metadata map[string]interface{}) (string, []*types.SigningPayload, error) {
	printInfo("validating /construction/payloads ...\n")
	output := checkSpecConstructionOutput[constructionPayloads]
	defer printInfo("/construction/payloads validated\n")

	unsignedTransaction, payloads, err := cs.coordinatorHelper.Payloads(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		metadata,
		nil,
	)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return "", nil, fmt.Errorf("%w: unable to send payloads request \n", err)
	}
	var requirementErr error
	if len(unsignedTransaction) == 0 {
		requirementErr = fmt.Errorf("unsigned transaction is required \n")
		setValidationStatus(output, unsignedTx, checkSpecFailure)
	}
	if len(payloads) == 0 {
		requirementErr = fmt.Errorf("payloads is required")
		setValidationStatus(output, signingPayloads, checkSpecFailure)
	}
	if requirementErr != nil {
		return "", nil, requirementErr
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return unsignedTransaction, payloads, nil
}

func (cs *checkSpec) ConstructionParse(ctx context.Context, broadcast *job.Broadcast, unsignedTransaction string) error {
	printInfo("validating /construction/parse ...\n")
	output := checkSpecConstructionOutput[constructionParse]
	defer printInfo("/construction/parse validated\n")

	_, _, _, err := cs.coordinatorHelper.Parse(ctx, broadcast.Network, false, unsignedTransaction)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return fmt.Errorf("%w: unable to send parse request \n", err)
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return nil
}

func (cs *checkSpec) ConstructionCombine(ctx context.Context, broadcast *job.Broadcast, unsignedTransaction string, signatures []*types.Signature) (string, error) {
	printInfo("validating /construction/combine ...\n")
	output := checkSpecConstructionOutput[constructionCombine]
	defer printInfo("/construction/combine validated\n")

	networkTransaction, err := cs.coordinatorHelper.Combine(ctx, broadcast.Network, unsignedTransaction, signatures)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return "", fmt.Errorf("%w: unable to send combine request \n", err)
	}
	if len(networkTransaction) == 0 {
		markAllValidationsStatus(output, checkSpecFailure)
		return "", fmt.Errorf("%w: signed_transaction is required \n", err)
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return networkTransaction, err
}

func (cs *checkSpec) ConstructionHash(ctx context.Context, broadcast *job.Broadcast, networkTransaction string) (*types.TransactionIdentifier, error) {
	printInfo("validating /construction/hash ...\n")
	output := checkSpecConstructionOutput[constructionHash]
	defer printInfo("/construction/hash validated\n")

	res, err := cs.coordinatorHelper.Hash(ctx, broadcast.Network, networkTransaction)
	if err != nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return nil, fmt.Errorf("%w: unable to send hash request", err)
	}
	if res == nil {
		markAllValidationsStatus(output, checkSpecFailure)
		return nil, fmt.Errorf("%w: transaction_identifier is required", err)
	}
	markAllValidationsStatus(output, checkSpecSuccess)
	return res, err
}

func (cs *checkSpec) checkConstruction(ctx context.Context) map[checkSpecAPI]checkSpecOutput {
	if len(Config.Construction.PrefundedAccounts) == 0 {
		printError("no prefunded account provided")
		return checkSpecConstructionOutput
	}

	// Get keypair from prefunded account
	curveType := Config.Construction.PrefundedAccounts[0].CurveType
	prefundedAccount := Config.Construction.PrefundedAccounts[0]
	senderKeypair, err := keys.ImportPrivateKey(prefundedAccount.PrivateKeyHex, curveType)
	if err != nil {
		printError("%v: failed to generate key pair from prefunded account: %v", err, prefundedAccount.AccountIdentifier)
		return checkSpecConstructionOutput
	}

	// Generate broadcast from transfer workflow in DSL file
	workflows := Config.Construction.Workflows
	var broadcast *job.Broadcast
	for _, workflow := range workflows {
		if workflow.Name == "transfer" {
			broadcast, err = cs.parseTransferFromDSL(ctx, workflow)
			if err != nil {
				printError("%w: failed to parse transfer workflow from DSL file", err)
				return checkSpecConstructionOutput
			}
		}
	}

	// Generate transaction
	metadata, err := cs.ConstructionPreprocess(ctx, broadcast)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	metadata, err = cs.ConstructionMetadata(ctx, broadcast, metadata, nil)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	unsignedTransaction, payloads, err := cs.ConstructionPayloads(ctx, broadcast, metadata)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	err = cs.ConstructionParse(ctx, broadcast, unsignedTransaction)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	signatures, err := sign(payloads, senderKeypair)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	networkTransaction, err := cs.ConstructionCombine(ctx, broadcast, unsignedTransaction, signatures)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}

	signedTxID, err := cs.ConstructionHash(ctx, broadcast, networkTransaction)
	if err != nil {
		printError(err.Error())
		return checkSpecConstructionOutput
	}
	fmt.Printf("+%v \n", signedTxID)

	// TODO: constructionSubmit
	return checkSpecConstructionOutput
}

func (cs *checkSpec) parseTransferFromDSL(ctx context.Context, workflow *job.Workflow) (*job.Broadcast, error) {
	job := job.New(workflow)
	// Increase by 1 so that broadcast instance can be generated successfully
	job.Index = 1

	var state string
	// Parsing actions, similar to:
	// https://github.com/coinbase/rosetta-sdk-go/blob/master/constructor/worker/worker.go#L92-L140
	for _, scenario := range workflow.Scenarios {
		state, err := cs.ProcessNextScenario(ctx, scenario, state)
		if err != nil {
			return nil, err
		}
		job.State = state
	}

	broadcast, err := job.CreateBroadcast()
	if err != nil {
		return nil, err
	}
	return broadcast, nil
}

func (cs *checkSpec) ProcessNextScenario(ctx context.Context, scenario *job.Scenario, state string) (string, error) {
	for _, action := range scenario.Actions {
		processedInput, err := worker.PopulateInput(state, action.Input)
		if err != nil {
			return "", fmt.Errorf("%w: unable to populate variables", err)
		}

		output, err := cs.invokeWorker(ctx, action.Type, processedInput)
		if err != nil {
			return "", fmt.Errorf("%w: unable to process action", err)
		}

		if len(output) == 0 {
			continue
		}

		state, err = sjson.SetRaw(state, action.OutputPath, output)
		if err != nil {
			return "", fmt.Errorf("%w: unable to update state", err)
		}
	}
	return state, nil
}

// invokeWorker function is similar to https://github.com/coinbase/rosetta-sdk-go/blob/master/constructor/worker/worker.go#L49-L90
// This new function avoids using any db operation, and it's specifically for Transfer workflow
func (cs *checkSpec) invokeWorker(
	ctx context.Context,
	action job.ActionType,
	input string,
) (string, error) {
	switch action {
	case job.SetVariable:
		return input, nil
	case job.PrintMessage:
		worker.PrintMessageWorker(input)
		return "", nil
	case job.RandomString:
		return worker.RandomStringWorker(input)
	case job.Math:
		return worker.MathWorker(input)
	case job.FindBalance:
		var balanceInput job.FindBalanceInput
		err := job.UnmarshalInput([]byte(input), &balanceInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", worker.ErrInvalidInput, err.Error())
		}
		return cs.findBalanceWorker(ctx, &balanceInput)
	case job.RandomNumber:
		return worker.RandomNumberWorker(input)
	case job.Assert:
		return "", worker.AssertWorker(input)
	case job.FindCurrencyAmount:
		return worker.FindCurrencyAmountWorker(input)
	case job.LoadEnv:
		return worker.LoadEnvWorker(input)
	case job.HTTPRequest:
		return worker.HTTPRequestWorker(input)
	default:
		return "", fmt.Errorf("%w: %s", worker.ErrInvalidActionType, action)
	}
}

func (cs *checkSpec) findBalanceWorker(
	ctx context.Context,
	input *job.FindBalanceInput,
) (string, error) {
	res, err := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if err != nil {
		return "", fmt.Errorf("%v: unable to get network status", err.Err)
	}
	currentBlockID := res.CurrentBlockIdentifier
	currentPartialBlockID := types.PartialBlockIdentifier{Hash: &currentBlockID.Hash}

	if input.MinimumBalance.Value == "0" {
		return cs.generateRecipientAccount(ctx)
	}
	if Config.CoinSupported {
		res, err := cs.checkAccountCoins(ctx, input)
		if err != nil {
			return "", err
		}
		return res, nil
	} else {
		res, err := cs.checkAccountBalance(ctx, input, currentPartialBlockID)
		if err != nil {
			return "", err
		}
		return res, nil
	}
}

func (cs *checkSpec) generateRecipientAccount(ctx context.Context) (string, error) {
	// Generate keypair for recipient
	curveType := Config.Construction.PrefundedAccounts[0].CurveType
	recipientKeypair, err := keys.GenerateKeypair(curveType)
	if err != nil {
		printError("%v: failed to generate key pair from curve type: %v", err, curveType)
		return "", err
	}

	// Generate address from recipient pk
	recipientAccount, _, err := cs.coordinatorHelper.Derive(ctx,
		Config.Network,
		recipientKeypair.PublicKey,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("%w: unable to generate recipient account", err)
	}
	return types.PrintStruct(&job.FindBalanceOutput{
		AccountIdentifier: recipientAccount,
	}), nil
}

func (cs *checkSpec) checkAccountBalance(
	ctx context.Context,
	input *job.FindBalanceInput,
	currentPartialBlockID types.PartialBlockIdentifier,
) (string, error) {
	for _, account := range Config.Construction.PrefundedAccounts {
		currencies := []*types.Currency{account.Currency}
		_, amounts, _, err := cs.onlineFetcher.AccountBalanceRetry(ctx, Config.Network, account.AccountIdentifier, &currentPartialBlockID, currencies)
		if err != nil {
			return "", err.Err
		}

		for _, amount := range amounts {
			// look for amounts > min
			diff, err := types.SubtractValues(amount.Value, input.MinimumBalance.Value)
			if err != nil {
				return "", fmt.Errorf("%w: %s", worker.ErrActionFailed, err.Error())
			}

			bigIntDiff, err := types.BigInt(diff)
			if err != nil {
				return "", fmt.Errorf("%w: %s", worker.ErrActionFailed, err.Error())
			}

			if bigIntDiff.Sign() < 0 {
				log.Printf(
					"checkAccountBalance: Account (%s) has balance (%s), less than the minimum balance (%s)",
					account.AccountIdentifier.Address,
					amount.Value,
					input.MinimumBalance.Value,
				)
				return "", nil
			}

			return types.PrintStruct(&job.FindBalanceOutput{
				AccountIdentifier: account.AccountIdentifier,
				Balance:           amount,
			}), nil
		}
	}
	return "", fmt.Errorf("no prefunded accounts have enough fund")
}

func (cs *checkSpec) checkAccountCoins(
	ctx context.Context,
	input *job.FindBalanceInput,
) (string, error) {
	for _, account := range Config.Construction.PrefundedAccounts {
		currencies := []*types.Currency{account.Currency}
		_, coins, _, err := cs.onlineFetcher.AccountCoinsRetry(ctx, Config.Network, account.AccountIdentifier, false, currencies)
		if err != nil {
			return "", err.Err
		}
		var disallowedCoins []string
		for _, coinIdentifier := range input.NotCoins {
			disallowedCoins = append(disallowedCoins, types.Hash(coinIdentifier))
		}
		for _, coin := range coins {
			if utils.ContainsString(disallowedCoins, types.Hash(coin.CoinIdentifier)) {
				continue
			}
			if coin.Amount.Value < input.MinimumBalance.Value || coin.Amount.Currency != input.MinimumBalance.Currency {
				continue
			}
			diff, err := types.SubtractValues(coin.Amount.Value, input.MinimumBalance.Value)
			if err != nil {
				return "", fmt.Errorf("%w: %s", worker.ErrActionFailed, err.Error())
			}

			bigIntDiff, err := types.BigInt(diff)
			if err != nil {
				return "", fmt.Errorf("%w: %s", worker.ErrActionFailed, err.Error())
			}

			if bigIntDiff.Sign() < 0 {
				continue
			}

			return types.PrintStruct(&job.FindBalanceOutput{
				AccountIdentifier: account.AccountIdentifier,
				Balance:           coin.Amount,
				Coin:              coin.CoinIdentifier,
			}), nil
		}
	}
	return "", fmt.Errorf("no prefunded accounts have enough fund")
}

func runCheckSpecCmd(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	cs, err := newCheckSpec(ctx)
	if err != nil {
		return fmt.Errorf("%v: unable to create checkSpec object with online URL", err)
	}

	output := []checkSpecOutput{}
	// validate api endpoints
	output = append(output, cs.networkStatus(ctx))
	output = append(output, cs.networkList(ctx))
	output = append(output, cs.networkOptions(ctx))
	output = append(output, cs.accountBalance(ctx))
	output = append(output, cs.accountCoins(ctx))
	output = append(output, cs.block(ctx))
	output = append(output, cs.errorObject(ctx))
	output = append(output, twoModes())

	cs.checkConstruction(ctx)
	for _, o := range checkSpecConstructionOutput {
		printCheckSpecOutputBody(o)
	}

	printInfo("check:spec is complete\n")
	printCheckSpecOutputHeader()
	for _, o := range output {
		printCheckSpecOutputBody(o)
	}

	return nil
}
