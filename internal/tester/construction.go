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

package tester

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"reflect"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/processor"
	"github.com/coinbase/rosetta-cli/internal/scenario"
	"github.com/coinbase/rosetta-cli/internal/statefulsyncer"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	// constructionCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	constructionCmdName = "check-construction"

	// defaultSleepTime is the default time we sleep
	// while waiting to perform the next task.
	defaultSleepTime = 10
)

// ConstructionTester coordinates the `check:construction` test.
type ConstructionTester struct {
	network              *types.NetworkIdentifier
	database             storage.Database
	config               *configuration.Configuration
	syncer               *statefulsyncer.StatefulSyncer
	logger               *logger.Logger
	counterStorage       *storage.CounterStorage
	keyStorage           *storage.KeyStorage
	broadcastStorage     *storage.BroadcastStorage
	blockStorage         *storage.BlockStorage
	coinStorage          *storage.CoinStorage
	balanceStorage       *storage.BalanceStorage
	balanceStorageHelper *processor.BalanceStorageHelper
	parser               *parser.Parser

	// parsed configuration
	minimumBalance *big.Int
	maximumFee     *big.Int
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
}

// CloseDatabase closes the database used by ConstructionTester.
func (t *ConstructionTester) CloseDatabase(ctx context.Context) {
	if err := t.database.Close(ctx); err != nil {
		log.Fatalf("%s: error closing database", err.Error())
	}
}

// StartPeriodicLogger prints out periodic
// stats about a run of `check:construction`.
func (t *ConstructionTester) StartPeriodicLogger(
	ctx context.Context,
) error {
	for ctx.Err() == nil {
		inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
		_ = t.logger.LogConstructionStats(ctx, len(inflight))
		time.Sleep(PeriodicLoggingFrequency)
	}

	// Print stats one last time before exiting
	inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
	_ = t.logger.LogConstructionStats(ctx, len(inflight))

	return ctx.Err()
}

// InitializeConstruction initiates the construction API tester.
func InitializeConstruction(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	onlineFetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
) (*ConstructionTester, error) {
	dataPath, err := utils.CreateCommandPath(config.DataDirectory, constructionCmdName, network)
	if err != nil {
		log.Fatalf("%s: cannot create command path", err.Error())
	}

	localStore, err := storage.NewBadgerStorage(ctx, dataPath)
	if err != nil {
		log.Fatalf("%s: unable to initialize database", err.Error())
	}

	counterStorage := storage.NewCounterStorage(localStore)
	logger := logger.NewLogger(
		counterStorage,
		dataPath,
		false,
		false,
		false,
		false,
	)

	blockStorage := storage.NewBlockStorage(localStore)
	keyStorage := storage.NewKeyStorage(localStore)
	coinStorage := storage.NewCoinStorage(localStore, onlineFetcher.Asserter)
	balanceStorage := storage.NewBalanceStorage(localStore)

	balanceStorageHelper := processor.NewBalanceStorageHelper(
		network,
		onlineFetcher,
		false,
		nil,
		true,
	)

	balanceStorageHandler := processor.NewBalanceStorageHandler(
		logger,
		nil,
		false,
		nil,
	)

	balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

	broadcastStorage := storage.NewBroadcastStorage(
		localStore,
		config.Construction.ConfirmationDepth,
		config.Construction.StaleDepth,
		config.Construction.BroadcastLimit,
		config.Construction.BroadcastTrailLimit,
		config.Construction.BlockBroadcastLimit,
	)

	broadcastHelper := processor.NewBroadcastStorageHelper(
		network,
		blockStorage,
		onlineFetcher,
	)
	parser := parser.New(onlineFetcher.Asserter, nil)
	broadcastHandler := processor.NewBroadcastStorageHandler(
		config,
		counterStorage,
		parser,
	)

	broadcastStorage.Initialize(broadcastHelper, broadcastHandler)

	minimumBalance, ok := new(big.Int).SetString(config.Construction.MinimumBalance, 10)
	if !ok {
		return nil, errors.New("cannot parse minimum balance")
	}

	maximumFee, ok := new(big.Int).SetString(config.Construction.MaximumFee, 10)
	if !ok {
		return nil, errors.New("cannot parse maximum fee")
	}

	offlineFetcher := fetcher.New(
		config.Construction.OfflineURL,
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
	)

	// Load all accounts for network
	addresses, err := keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to load addresses", err)
	}

	log.Printf("construction tester initialized with %d addresses\n", len(addresses))

	// Track balances on all addresses
	for _, address := range addresses {
		balanceStorageHelper.AddInterestingAddress(address)
	}

	syncer := statefulsyncer.New(
		ctx,
		network,
		onlineFetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]storage.BlockWorker{balanceStorage, coinStorage, broadcastStorage},
	)

	return &ConstructionTester{
		network:              network,
		database:             localStore,
		config:               config,
		syncer:               syncer,
		logger:               logger,
		counterStorage:       counterStorage,
		keyStorage:           keyStorage,
		broadcastStorage:     broadcastStorage,
		blockStorage:         blockStorage,
		coinStorage:          coinStorage,
		balanceStorage:       balanceStorage,
		balanceStorageHelper: balanceStorageHelper,
		parser:               parser,
		minimumBalance:       minimumBalance,
		maximumFee:           maximumFee,
		onlineFetcher:        onlineFetcher,
		offlineFetcher:       offlineFetcher,
	}, nil
}

// CreateTransaction constructs and signs a transaction with the provided intent.
func (t *ConstructionTester) CreateTransaction(
	ctx context.Context,
	intent []*types.Operation,
) (*types.TransactionIdentifier, string, error) {
	metadataRequest, err := t.offlineFetcher.ConstructionPreprocess(
		ctx,
		t.network,
		intent,
		nil,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to preprocess", err)
	}

	requiredMetadata, err := t.onlineFetcher.ConstructionMetadata(
		ctx,
		t.network,
		metadataRequest,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct metadata", err)
	}

	unsignedTransaction, payloads, err := t.offlineFetcher.ConstructionPayloads(
		ctx,
		t.network,
		intent,
		requiredMetadata,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct payloads", err)
	}

	parsedOps, _, _, err := t.offlineFetcher.ConstructionParse(
		ctx,
		t.network,
		false,
		unsignedTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse unsigned transaction", err)
	}

	if err := t.parser.ExpectedOperations(intent, parsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: unsigned parsed ops do not match intent", err)
	}

	signatures, err := t.keyStorage.Sign(ctx, payloads)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to sign payloads", err)
	}

	networkTransaction, err := t.offlineFetcher.ConstructionCombine(
		ctx,
		t.network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to combine signatures", err)
	}

	signedParsedOps, signers, _, err := t.offlineFetcher.ConstructionParse(
		ctx,
		t.network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse signed transaction", err)
	}

	if err := t.parser.ExpectedOperations(intent, signedParsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: signed parsed ops do not match intent", err)
	}

	if err := parser.ExpectedSigners(payloads, signers); err != nil {
		return nil, "", fmt.Errorf("%w: signed transactions signers do not match intent", err)
	}

	transactionIdentifier, err := t.offlineFetcher.ConstructionHash(
		ctx,
		t.network,
		networkTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to get transaction hash", err)
	}

	return transactionIdentifier, networkTransaction, nil
}

// StartSyncer uses the tester's stateful syncer
// to compute balance changes and track transactions
// for confirmation on-chain.
func (t *ConstructionTester) StartSyncer(
	ctx context.Context,
	cancel context.CancelFunc,
) error {
	startIndex := int64(-1)
	_, err := t.blockStorage.GetHeadBlockIdentifier(ctx)
	if errors.Is(err, storage.ErrHeadBlockNotFound) {
		// If a block has yet to be synced, start syncing from tip.
		// TODO: make configurable
		status, err := t.onlineFetcher.NetworkStatusRetry(ctx, t.network, nil)
		if err != nil {
			return fmt.Errorf("%w: unable to fetch network status", err)
		}

		startIndex = status.CurrentBlockIdentifier.Index
	} else if err != nil {
		return fmt.Errorf("%w: unable to get last block synced", err)
	}

	return t.syncer.Sync(ctx, startIndex, -1)
}

// NewAddress generates a new keypair and
// derives its address offline. This only works
// for blockchains that don't require an on-chain
// action to create an account.
func (t *ConstructionTester) NewAddress(
	ctx context.Context,
) (string, error) {
	kp, err := keys.GenerateKeypair(t.config.Construction.CurveType)
	if err != nil {
		return "", fmt.Errorf("%w unable to generate keypair", err)
	}

	address, _, err := t.offlineFetcher.ConstructionDerive(
		ctx,
		t.network,
		kp.PublicKey,
		nil,
	)

	if err != nil {
		return "", fmt.Errorf("%w: unable to derive address", err)
	}

	err = t.keyStorage.Store(ctx, address, kp)
	if err != nil {
		return "", fmt.Errorf("%w: unable to store address", err)
	}

	t.balanceStorageHelper.AddInterestingAddress(address)

	_, _ = t.counterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return address, nil
}

// RequestFunds prompts the user to load
// a particular address with funds from a faucet.
// TODO: automate this using an API faucet.
func (t *ConstructionTester) RequestFunds(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	printedMessage := false
	for ctx.Err() == nil {
		sendableBalance, coinIdentifier, err := t.SendableBalance(ctx, address)
		if err != nil {
			return nil, nil, err
		}

		if sendableBalance != nil {
			log.Printf("found sendable balance %s on %s\n", sendableBalance.String(), address)
			return sendableBalance, coinIdentifier, nil
		}

		if !printedMessage {
			color.Yellow("waiting for funds on %s", address)
			printedMessage = true
		}
		time.Sleep(defaultSleepTime * time.Second)
	}

	return nil, nil, ctx.Err()
}

func (t *ConstructionTester) standardDeduction() *big.Int {
	// We should only consider sending an amount after
	// taking out the minimum balance for the sender,
	// the minimum balance for the receiver,
	// and the maximum fee.
	//
	// TODO: send amount to recipient below minimumBalance
	// if account has > minimumBalance
	standardDeduction := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
	if t.config.Construction.AccountingModel == configuration.UtxoModel {
		// UTXOs destroy the sending coin so it doesn't need to maintain
		// a minimum balance.
		standardDeduction = t.minimumBalance
	}
	return new(big.Int).Add(standardDeduction, t.maximumFee)
}

// SendableBalance returns the amount to use for
// a transfer. In the case of a UTXO-based chain,
// this is the largest remaining UTXO.
func (t *ConstructionTester) SendableBalance(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	accountIdentifier := &types.AccountIdentifier{Address: address}

	var bal *big.Int
	var coinIdentifier *types.CoinIdentifier
	switch t.config.Construction.AccountingModel {
	case configuration.AccountModel:
		amount, _, err := t.balanceStorage.GetBalance(
			ctx,
			accountIdentifier,
			t.config.Construction.Currency,
			nil,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to fetch balance for %s", err, address)
		}

		val, ok := new(big.Int).SetString(amount.Value, 10)
		if !ok {
			return nil, nil, fmt.Errorf("could not parse amount for %s", address)
		}

		bal = val
	case configuration.UtxoModel:
		// For UTXO-based chains, return the largest UTXO as the spendable balance.
		coins, err := t.coinStorage.GetCoins(ctx, accountIdentifier)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get utxo balance for %s", err, address)
		}

		balance := big.NewInt(0)
		for _, coin := range coins {
			if types.Hash(coin.Operation.Amount.Currency) != types.Hash(t.config.Construction.Currency) {
				continue
			}

			val, ok := new(big.Int).SetString(coin.Operation.Amount.Value, 10)
			if !ok {
				return nil, nil, fmt.Errorf("could not parse amount for coin %s", coin.Identifier.Identifier)
			}

			if balance.Cmp(val) == -1 {
				balance = val
				coinIdentifier = coin.Identifier
			}
		}

		bal = balance
	default:
		// We should never hit this branch because the configuration file is
		// checked for issues like this before starting this loop.
		return nil, nil, fmt.Errorf("invalid accounting model %s", t.config.Construction.AccountingModel)
	}

	if new(big.Int).Sub(bal, t.standardDeduction()).Sign() != 1 {
		return nil, nil, nil
	}

	return bal, coinIdentifier, nil
}

// FindSender fetches all available addresses,
// all locked addresses, and all address balances
// to determine which addresses can facilitate
// a transfer. If any are available, one is
// randomly selected.
func (t *ConstructionTester) FindSender(
	ctx context.Context,
) (string, *big.Int, *types.CoinIdentifier, error) {
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: unable to get addresses", err)
	}

	if len(addresses) == 0 { // create new and load
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to create address", err)
		}
		spendableBalance, coinIdentifier, err := t.RequestFunds(ctx, addr)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get load", err)
		}

		return addr, spendableBalance, coinIdentifier, nil
	}

	unlockedAddresses := []string{}
	lockedAddresses, err := t.broadcastStorage.LockedAddresses(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: unable to get locked addresses", err)
	}

	// Convert to a map so can do fast lookups
	lockedSet := map[string]struct{}{}
	for _, address := range lockedAddresses {
		lockedSet[address] = struct{}{}
	}

	for _, address := range addresses {
		if _, exists := lockedSet[address]; !exists {
			unlockedAddresses = append(unlockedAddresses, address)
		}
	}

	// Only check addresses not currently locked
	type compoundAmount struct {
		amount         *big.Int
		coinIdentifier *types.CoinIdentifier
	}

	sendableAddresses := map[string]*compoundAmount{}
	for _, address := range unlockedAddresses {
		sendableBalance, coinIdentifier, err := t.SendableBalance(ctx, address)
		if err != nil {
			return "", nil, nil, err
		}

		if sendableBalance == nil {
			continue
		}

		sendableAddresses[address] = &compoundAmount{
			amount:         sendableBalance,
			coinIdentifier: coinIdentifier,
		}
	}

	if len(sendableAddresses) > 0 {
		sendableKeys := reflect.ValueOf(sendableAddresses).MapKeys()
		addr := sendableKeys[rand.Intn(len(sendableKeys))].String()

		return addr, sendableAddresses[addr].amount, sendableAddresses[addr].coinIdentifier, nil
	}

	broadcasts, err := t.broadcastStorage.GetAllBroadcasts(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	if len(broadcasts) > 0 {
		// If there are pending broadcasts, don't attempt to load more funds!
		// We should instead wait for existing funds to be sent around.
		return "", nil, nil, nil
	}

	addr, err := t.keyStorage.RandomAddress(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: unable to get random address", err)
	}

	sendableBalance, coinIdentifier, err := t.RequestFunds(ctx, addr)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w: unable to get load", err)
	}

	return addr, sendableBalance, coinIdentifier, nil
}

// FindRecipient either finds a random existing
// recipient or generates a new address.
func (t *ConstructionTester) FindRecipient(
	ctx context.Context,
	sender string,
	forceNew bool,
) (string, error) {
	validRecipients := []string{}
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get address", err)
	}
	for _, a := range addresses {
		if a == sender {
			continue
		}

		validRecipients = append(validRecipients, a)
	}

	// Randomly generate new recipients
	coinFlip := false
	if rand.Float64() > t.config.Construction.NewAccountProbability &&
		len(addresses) < t.config.Construction.MaxAddresses {
		coinFlip = true
	}

	if len(validRecipients) == 0 || coinFlip || forceNew {
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate new address", err)
		}

		return addr, nil
	}

	return validRecipients[rand.Intn(len(validRecipients))], nil
}

// CreateScenarioContext creates the context to use
// for scenario population.
func (t *ConstructionTester) CreateScenarioContext(
	ctx context.Context,
	sender string,
	recipient string,
	sendableBalance *big.Int,
	coinIdentifier *types.CoinIdentifier,
	scenarioOps []*types.Operation,
) (*scenario.Context, []*types.Operation, error) {
	sendable := new(big.Int).Sub(sendableBalance, t.standardDeduction())

	// [0, sendableBalance - t.standardDeduction())
	// Account-based blockchains: [0, sendableBalance - 2*minimum_balance - maximum_fee)
	// UTXO-based blockchains: [0, sendableBalance - minimum_balance - maximum_fee)
	unallocatedTransferValue := new(big.Int).Rand(rand.New(rand.NewSource(time.Now().Unix())), sendable)

	// Add back minimum balance
	recipientValue := new(big.Int).Add(unallocatedTransferValue, t.minimumBalance)

	var senderValue, changeValue *big.Int
	var changeAddress string
	var err error
	switch t.config.Construction.AccountingModel {
	case configuration.AccountModel:
		senderValue = recipientValue
	case configuration.UtxoModel:
		senderValue = sendableBalance

		// Attempt to create a change output if we should produce change.
		if t.config.Construction.ChangeIntent != nil {
			// We consider the changableSurplus to be anything above the minimum
			// balance of the recipient and the change address.
			minimumBalances := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
			changableSurplus := new(big.Int).Sub(recipientValue, minimumBalances)

			// If the changableSurplus is positive, we attempt to send a change
			// output. If not, we don't add ChangeIntent to the scenario.
			if changableSurplus.Sign() == 1 {
				// The recipient value is recipientSplit + minimumBalance and the
				// change value is (changableSurplus - recipientSplit) + minimumBalance.
				// Note that the changableSurplus already takes into account the fee.
				recipientSplit := new(
					big.Int,
				).Rand(
					rand.New(rand.NewSource(time.Now().Unix())),
					changableSurplus,
				)
				recipientValue = new(big.Int).Add(recipientSplit, t.minimumBalance)
				changeSplit := new(big.Int).Sub(changableSurplus, recipientSplit)
				changeValue = new(big.Int).Add(changeSplit, t.minimumBalance)

				changeAddress, err = t.FindRecipient(ctx, sender, true)
				if err != nil {
					return nil, nil, fmt.Errorf("%w: unable to find change recipient", err)
				}

				scenarioOps = append(scenarioOps, t.config.Construction.ChangeIntent)
			}
		}
	default:
		// We should never hit this branch because the configuration file is
		// checked for issues like this before starting this loop.
		return nil, nil, fmt.Errorf("invalid accounting model %s", t.config.Construction.AccountingModel)
	}

	return &scenario.Context{
		Sender:         sender,
		SenderValue:    senderValue,
		Recipient:      recipient,
		RecipientValue: recipientValue,
		Currency:       t.config.Construction.Currency,
		CoinIdentifier: coinIdentifier,
		ChangeAddress:  changeAddress,
		ChangeValue:    changeValue,
	}, scenarioOps, nil
}

// LogTransaction logs what a scenario is perfoming
// to the console.
func (t *ConstructionTester) LogTransaction(
	scenarioCtx *scenario.Context,
	transactionIdentifier *types.TransactionIdentifier,
) {
	divisor := utils.BigPow10(t.config.Construction.Currency.Decimals)
	precision := int(t.config.Construction.Currency.Decimals)
	if len(scenarioCtx.ChangeAddress) == 0 {
		nativeUnits := new(big.Float).SetInt(scenarioCtx.RecipientValue)
		nativeUnits = new(big.Float).Quo(nativeUnits, divisor)

		color.Magenta(
			"Transaction Created: %s\n  %s -- %s %s --> %s",
			transactionIdentifier.Hash,
			scenarioCtx.Sender,
			nativeUnits.Text('f', precision),
			t.config.Construction.Currency.Symbol,
			scenarioCtx.Recipient,
		)
	} else {
		recipientUnits := new(big.Float).SetInt(scenarioCtx.RecipientValue)
		recipientUnits = new(big.Float).Quo(recipientUnits, divisor)

		changeUnits := new(big.Float).SetInt(scenarioCtx.ChangeValue)
		changeUnits = new(big.Float).Quo(changeUnits, divisor)
		color.Magenta(
			"Transaction Created: %s\n  %s\n    -- %s %s --> %s\n    -- %s %s --> %s",
			transactionIdentifier.Hash,
			scenarioCtx.Sender,
			recipientUnits.Text('f', precision),
			t.config.Construction.Currency.Symbol,
			scenarioCtx.Recipient,
			changeUnits.Text('f', precision),
			t.config.Construction.Currency.Symbol,
			scenarioCtx.ChangeAddress,
		)
	}
}

// CreateTransactions loops on the create transaction loop
// until the caller cancels the context.
func (t *ConstructionTester) CreateTransactions(ctx context.Context) error {
	// Before starting loop, delete any pending broadcasts if configuration
	// indicates to do so.
	if t.config.Construction.ClearBroadcasts {
		broadcasts, err := t.broadcastStorage.ClearBroadcasts(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to clear broadcasts", err)
		}

		log.Printf(
			"Cleared pending %d broadcasts: %s\n",
			len(broadcasts),
			types.PrettyPrintStruct(broadcasts),
		)
	}

	for ctx.Err() == nil {
		sender, sendableBalance, coinIdentifier, err := t.FindSender(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		if len(sender) == 0 {
			// This condition occurs when we are waiting for some
			// pending broadcast to complete before creating more
			// transactions.

			time.Sleep(defaultSleepTime * time.Second)
			continue
		}

		recipient, err := t.FindRecipient(ctx, sender, false)
		if err != nil {
			return fmt.Errorf("%w: unable to find recipient", err)
		}

		scenarioCtx, scenarioOps, err := t.CreateScenarioContext(
			ctx,
			sender,
			recipient,
			sendableBalance,
			coinIdentifier,
			t.config.Construction.Scenario,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to create scenario context", err)
		}

		intent, err := scenario.PopulateScenario(ctx, scenarioCtx, scenarioOps)
		if err != nil {
			return fmt.Errorf("%w: unable to populate scenario", err)
		}

		// Create transaction
		transactionIdentifier, networkTransaction, err := t.CreateTransaction(ctx, intent)
		if err != nil {
			return fmt.Errorf(
				"%w: unable to create transaction with operations %s",
				err,
				types.PrettyPrintStruct(intent),
			)
		}

		t.LogTransaction(scenarioCtx, transactionIdentifier)

		// Broadcast Transaction
		err = t.broadcastStorage.Broadcast(
			ctx,
			sender,
			intent,
			transactionIdentifier,
			networkTransaction,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to enqueue transaction for broadcast", err)
		}

		_, _ = t.logger.CounterStorage.Update(
			ctx,
			storage.TransactionsCreatedCounter,
			big.NewInt(1),
		)
	}

	return ctx.Err()
}

// PerformBroadcasts attempts to rebroadcast all pending transactions
// if the RebroadcastAll configuration is set to true.
func (t *ConstructionTester) PerformBroadcasts(ctx context.Context) error {
	if !t.config.Construction.RebroadcastAll {
		return nil
	}

	color.Magenta("Rebroadcasting all transactions...")

	if err := t.broadcastStorage.BroadcastAll(ctx, false); err != nil {
		return fmt.Errorf("%w: unable to broadcast all transactions", err)
	}

	return nil
}
