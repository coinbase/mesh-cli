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
	"github.com/jinzhu/copier"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// action is some supported intent we can
// perform. This is used to determine the minimum
// required balance to complete the transaction.
type action string

const (
	// constructionCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	constructionCmdName = "check-construction"

	// defaultSleepTime is the default time we sleep
	// while waiting to perform the next task.
	defaultSleepTime = 10

	// newAccountSend is a send to a new account.
	newAccountSend action = "new-account-send"

	// ExistingAccountSend is a send to an existing account.
	existingAccountSend action = "existing-account-send"

	// changeSend is a send that creates a UTXO
	// for the recipient and sends the remainder
	// to a change UTXO.
	changeSend action = "change-send"

	// fullSend is a send that transfers
	// all value in one UTXO into another.
	fullSend action = "full-send"
)

var (
	// ErrInsufficientFunds is returned when we must
	// request funds.
	ErrInsufficientFunds = errors.New("insufficient funds")
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
		config.TipDelay,
		config.Construction.BroadcastBehindTip,
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
		sender, balance, coinIdentifier, err := t.findSender(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		// Determine Action
		scenarioCtx, scenarioOps, err := t.generateScenario(
			ctx,
			sender,
			balance,
			coinIdentifier,
		)
		if errors.Is(err, ErrInsufficientFunds) {
			broadcasts, err := t.broadcastStorage.GetAllBroadcasts(ctx)
			if err != nil {
				return fmt.Errorf("%w: unable to get broadcasts", err)
			}

			if len(broadcasts) > 0 {
				// we will wait for in-flight to process
				time.Sleep(defaultSleepTime * time.Second)
				continue
			}

			if err := t.generateNewAndRequest(ctx); err != nil {
				return fmt.Errorf("%w: unable to generate new address", err)
			}

			continue
		} else if err != nil {
			return fmt.Errorf("%w: unable to generate intent", err)
		}

		intent, err := scenario.PopulateScenario(ctx, scenarioCtx, scenarioOps)
		if err != nil {
			return fmt.Errorf("%w: unable to populate scenario", err)
		}

		// Create transaction
		transactionIdentifier, networkTransaction, err := t.createTransaction(ctx, intent)
		if err != nil {
			return fmt.Errorf(
				"%w: unable to create transaction with operations %s",
				err,
				types.PrettyPrintStruct(intent),
			)
		}

		logger.LogScenario(scenarioCtx, transactionIdentifier, t.config.Construction.Currency)

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

/*
 * Private Methods
 */

// createTransaction constructs and signs a transaction with the provided intent.
func (t *ConstructionTester) createTransaction(
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

// newAddress generates a new keypair and
// derives its address offline. This only works
// for blockchains that don't require an on-chain
// action to create an account.
func (t *ConstructionTester) newAddress(
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

// requestFunds prompts the user to load
// a particular address with funds from a faucet.
// TODO: automate this using an API faucet.
func (t *ConstructionTester) requestFunds(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	printedMessage := false
	for ctx.Err() == nil {
		balance, coinIdentifier, err := t.balance(ctx, address)
		if err != nil {
			return nil, nil, err
		}

		minBalance := t.minimumRequiredBalance(newAccountSend)
		if t.config.Construction.AccountingModel == configuration.UtxoModel {
			minBalance = t.minimumRequiredBalance(changeSend)
		}

		if balance != nil && new(big.Int).Sub(balance, minBalance).Sign() != -1 {
			color.Green("Found balance %s on %s", utils.PrettyAmount(balance, t.config.Construction.Currency), address)
			return balance, coinIdentifier, nil
		}

		if !printedMessage {
			color.Yellow("Waiting for funds on %s", address)
			printedMessage = true
		}
		time.Sleep(defaultSleepTime * time.Second)
	}

	return nil, nil, ctx.Err()
}

func (t *ConstructionTester) minimumRequiredBalance(action action) *big.Int {
	doubleMinimumBalance := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
	switch action {
	case newAccountSend, changeSend:
		// In this account case, we must have keep a balance above
		// the minimum_balance in the sender's account and send
		// an amount of at least the minimum_balance to the recipient.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the recipient and the change address (or
		// we will create dust).
		return new(big.Int).Add(doubleMinimumBalance, t.maximumFee)
	case existingAccountSend, fullSend:
		// In the account case, we must keep a balance above
		// the minimum_balance in the sender's account.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the new UTXO.
		return new(big.Int).Add(t.minimumBalance, t.maximumFee)
	}

	return nil
}

func (t *ConstructionTester) accountBalance(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) (*big.Int, *types.CoinIdentifier, error) {
	amount, _, err := t.balanceStorage.GetBalance(
		ctx,
		accountIdentifier,
		t.config.Construction.Currency,
		nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to fetch balance for %s",
			err,
			accountIdentifier.Address,
		)
	}

	val, ok := new(big.Int).SetString(amount.Value, 10)
	if !ok {
		return nil, nil, fmt.Errorf(
			"could not parse amount for %s",
			accountIdentifier.Address,
		)
	}

	return val, nil, nil
}

func (t *ConstructionTester) utxoBalance(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) (*big.Int, *types.CoinIdentifier, error) {
	// For UTXO-based chains, return the largest UTXO as the spendable balance.
	coins, err := t.coinStorage.GetCoins(ctx, accountIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to get utxo balance for %s",
			err,
			accountIdentifier.Address,
		)
	}

	bal := big.NewInt(0)
	var coinIdentifier *types.CoinIdentifier
	for _, coin := range coins {
		if types.Hash(
			coin.Operation.Amount.Currency,
		) != types.Hash(
			t.config.Construction.Currency,
		) {
			continue
		}

		val, ok := new(big.Int).SetString(coin.Operation.Amount.Value, 10)
		if !ok {
			return nil, nil, fmt.Errorf(
				"could not parse amount for coin %s",
				coin.Identifier.Identifier,
			)
		}

		if bal.Cmp(val) == -1 {
			bal = val
			coinIdentifier = coin.Identifier
		}
	}

	return bal, coinIdentifier, nil
}

// balance returns the total balance to use for
// a transfer. In the case of a UTXO-based chain,
// this is the largest remaining UTXO.
func (t *ConstructionTester) balance(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	accountIdentifier := &types.AccountIdentifier{Address: address}

	switch t.config.Construction.AccountingModel {
	case configuration.AccountModel:
		return t.accountBalance(ctx, accountIdentifier)
	case configuration.UtxoModel:
		return t.utxoBalance(ctx, accountIdentifier)
	}

	return nil, nil, fmt.Errorf("unable to find balance for %s", address)
}

func (t *ConstructionTester) getBestUnlockedSender(
	ctx context.Context,
	addresses []string,
) (
	string, // best address
	*big.Int, // best balance
	*types.CoinIdentifier, // best coin
	error,
) {
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
	var bestAddress string
	var bestBalance *big.Int
	var bestCoin *types.CoinIdentifier
	for _, address := range unlockedAddresses {
		balance, coinIdentifier, err := t.balance(ctx, address)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get balance for %s", err, address)
		}

		if bestBalance == nil || new(big.Int).Sub(bestBalance, balance).Sign() == -1 {
			bestAddress = address
			bestBalance = balance
			bestCoin = coinIdentifier
		}
	}

	return bestAddress, bestBalance, bestCoin, nil
}

// findSender fetches all available addresses,
// all locked addresses, and all address balances
// to determine which addresses can facilitate
// a transfer. The sender with the highest
// balance is returned (or the largest UTXO).
func (t *ConstructionTester) findSender(
	ctx context.Context,
) (
	string, // sender
	*big.Int, // balance
	*types.CoinIdentifier, // coin
	error,
) {
	for ctx.Err() == nil {
		addresses, err := t.keyStorage.GetAllAddresses(ctx)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get addresses", err)
		}

		if len(addresses) == 0 { // create new and load
			err := t.generateNewAndRequest(ctx)
			if err != nil {
				return "", nil, nil, fmt.Errorf("%w: unable to generate new and request", err)
			}

			continue // we will exit on next loop
		}

		bestAddress, bestBalance, bestCoin, err := t.getBestUnlockedSender(ctx, addresses)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get best unlocked sender", err)
		}

		if len(bestAddress) > 0 {
			return bestAddress, bestBalance, bestCoin, nil
		}

		broadcasts, err := t.broadcastStorage.GetAllBroadcasts(ctx)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get broadcasts", err)
		}

		if len(broadcasts) > 0 {
			// This condition occurs when we are waiting for some
			// pending broadcast to complete before creating more
			// transactions.

			time.Sleep(defaultSleepTime * time.Second)
			continue
		}

		if err := t.generateNewAndRequest(ctx); err != nil {
			return "", nil, nil, fmt.Errorf("%w: generate new address and request", err)
		}
	}

	return "", nil, nil, ctx.Err()
}

// findRecipients returns all possible
// recipients (address != sender).
func (t *ConstructionTester) findRecipients(
	ctx context.Context,
	sender string,
) (
	[]string, // recipients with minimum balance
	[]string, // recipients without minimum balance
	error,
) {
	minimumRecipients := []string{}
	belowMinimumRecipients := []string{}

	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get address", err)
	}
	for _, a := range addresses {
		if a == sender {
			continue
		}

		// Sending UTXOs always requires sending to the minimum.
		if t.config.Construction.AccountingModel == configuration.UtxoModel {
			belowMinimumRecipients = append(belowMinimumRecipients, a)

			continue
		}

		bal, _, err := t.balance(ctx, a)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to retrieve balance for %s", err, a)
		}

		if new(big.Int).Sub(bal, t.minimumBalance).Sign() >= 0 {
			minimumRecipients = append(minimumRecipients, a)

			continue
		}

		belowMinimumRecipients = append(belowMinimumRecipients, a)
	}

	return minimumRecipients, belowMinimumRecipients, nil
}

// createScenarioContext creates the context to use
// for scenario population.
func (t *ConstructionTester) createScenarioContext(
	sender string,
	senderValue *big.Int,
	recipient string,
	recipientValue *big.Int,
	changeAddress string,
	changeValue *big.Int,
	coinIdentifier *types.CoinIdentifier,
) (*scenario.Context, []*types.Operation, error) {
	// We create a deep copy of the scenaerio (and the change scenario)
	// to ensure we don't accidentally overwrite the loaded configuration
	// while hydrating values.
	scenarioOps := []*types.Operation{}
	if err := copier.Copy(&scenarioOps, t.config.Construction.Scenario); err != nil {
		return nil, nil, fmt.Errorf("%w: unable to copy scenario", err)
	}

	if len(changeAddress) > 0 {
		changeCopy := types.Operation{}
		if err := copier.Copy(&changeCopy, t.config.Construction.ChangeScenario); err != nil {
			return nil, nil, fmt.Errorf("%w: unable to copy change intent", err)
		}

		scenarioOps = append(scenarioOps, &changeCopy)
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

func (t *ConstructionTester) canGetNewAddress(
	ctx context.Context,
	recipients []string,
) (string, bool, error) {
	availableAddresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return "", false, fmt.Errorf("%w: unable to get available addresses", err)
	}

	if (rand.Float64() > t.config.Construction.NewAccountProbability &&
		len(availableAddresses) < t.config.Construction.MaxAddresses) || len(recipients) == 0 {
		addr, err := t.newAddress(ctx)
		if err != nil {
			return "", false, fmt.Errorf("%w: cannot create new address", err)
		}

		return addr, true, nil
	}

	return recipients[0], false, nil
}

func (t *ConstructionTester) generateAccountScenario(
	ctx context.Context,
	sender string,
	balance *big.Int,
	minimumRecipients []string,
	belowMinimumRecipients []string,
) (
	*scenario.Context,
	[]*types.Operation, // scenario operations
	error, // ErrInsufficientFunds
) {
	adjustedBalance := new(big.Int).Sub(balance, t.minimumBalance)

	// should send to new account, existing account, or no acccount?
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(newAccountSend)).Sign() != -1 {
		recipient, created, err := t.canGetNewAddress(
			ctx,
			append(minimumRecipients, belowMinimumRecipients...),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get recipient", err)
		}

		if created || utils.ContainsString(belowMinimumRecipients, recipient) {
			recipientValue := utils.RandomNumber(t.minimumBalance, adjustedBalance)
			return t.createScenarioContext(
				sender,
				recipientValue,
				recipient,
				recipientValue,
				"",
				nil,
				nil,
			)
		}

		// We do not need to send the minimum amount here because the recipient
		// already has a minimum balance.
		recipientValue := utils.RandomNumber(big.NewInt(0), adjustedBalance)
		return t.createScenarioContext(
			sender,
			recipientValue,
			recipient,
			recipientValue,
			"",
			nil,
			nil,
		)
	}

	recipientValue := utils.RandomNumber(big.NewInt(0), adjustedBalance)
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(existingAccountSend)).Sign() != -1 {
		if len(minimumRecipients) == 0 {
			return nil, nil, ErrInsufficientFunds
		}

		return t.createScenarioContext(
			sender,
			recipientValue,
			minimumRecipients[0],
			recipientValue,
			"",
			nil,
			nil,
		)
	}

	// Cannot perform any transfer.
	return nil, nil, ErrInsufficientFunds
}

func (t *ConstructionTester) generateUtxoScenario(
	ctx context.Context,
	sender string,
	balance *big.Int,
	recipients []string,
	coinIdentifier *types.CoinIdentifier,
) (
	*scenario.Context,
	[]*types.Operation, // scenario operations
	error, // ErrInsufficientFunds
) {
	feeLessBalance := new(big.Int).Sub(balance, t.maximumFee)
	recipient, created, err := t.canGetNewAddress(ctx, recipients)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get recipient", err)
	}

	// Need to remove from recipients if did not create a recipient address
	if !created {
		newRecipients := []string{}
		for _, r := range recipients {
			if recipient != r {
				newRecipients = append(newRecipients, r)
			}
		}

		recipients = newRecipients
	}

	// should send to change, no change, or no send?
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(changeSend)).Sign() != -1 &&
		t.config.Construction.ChangeScenario != nil {
		changeAddress, _, err := t.canGetNewAddress(ctx, recipients)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get change address", err)
		}

		doubleMinimumBalance := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
		changeDifferential := new(big.Int).Sub(feeLessBalance, doubleMinimumBalance)

		recipientShare := utils.RandomNumber(big.NewInt(0), changeDifferential)
		changeShare := new(big.Int).Sub(changeDifferential, recipientShare)

		recipientValue := new(big.Int).Add(t.minimumBalance, recipientShare)
		changeValue := new(big.Int).Add(t.minimumBalance, changeShare)

		return t.createScenarioContext(
			sender,
			balance,
			recipient,
			recipientValue,
			changeAddress,
			changeValue,
			coinIdentifier,
		)
	}

	if new(big.Int).Sub(balance, t.minimumRequiredBalance(fullSend)).Sign() != -1 {
		return t.createScenarioContext(
			sender,
			balance,
			recipient,
			utils.RandomNumber(t.minimumBalance, feeLessBalance),
			"",
			nil,
			nil,
		)
	}

	// Cannot perform any transfer.
	return nil, nil, ErrInsufficientFunds
}

// generateScenario determines what should be done in a given
// transfer based on the sender's balance.
func (t *ConstructionTester) generateScenario(
	ctx context.Context,
	sender string,
	balance *big.Int,
	coinIdentifier *types.CoinIdentifier,
) (
	*scenario.Context,
	[]*types.Operation, // scenario operations
	error, // ErrInsufficientFunds
) {
	minimumRecipients, belowMinimumRecipients, err := t.findRecipients(ctx, sender)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to find recipients", err)
	}

	switch t.config.Construction.AccountingModel {
	case configuration.AccountModel:
		return t.generateAccountScenario(
			ctx,
			sender,
			balance,
			minimumRecipients,
			belowMinimumRecipients,
		)
	case configuration.UtxoModel:
		return t.generateUtxoScenario(ctx, sender, balance, belowMinimumRecipients, coinIdentifier)
	}

	return nil, nil, ErrInsufficientFunds
}

func (t *ConstructionTester) generateNewAndRequest(ctx context.Context) error {
	addr, err := t.newAddress(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to create address", err)
	}

	_, _, err = t.requestFunds(ctx, addr)
	if err != nil {
		return fmt.Errorf("%w: unable to get funds on %s", err, addr)
	}

	return nil
}
