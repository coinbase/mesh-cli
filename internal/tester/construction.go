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

var (
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
		balance, coinIdentifier, err := t.Balance(ctx, address)
		if err != nil {
			return nil, nil, err
		}

		if balance != nil {
			color.Green("Found balance %s on %s", t.prettyAmount(balance), address)
			return balance, coinIdentifier, nil
		}

		if !printedMessage {
			color.Yellow("Waiting for funds on %s ...", address)
			printedMessage = true
		}
		time.Sleep(defaultSleepTime * time.Second)
	}

	return nil, nil, ctx.Err()
}

type Action string

const (
	// Account-based actions
	NewAccountSend      Action = "new-account-send"
	ExistingAccountSend Action = "existing-account-send"

	// UTXO-based actions
	ChangeSend Action = "change-send"
	FullSend   Action = "full-send"
)

func (t *ConstructionTester) minimumRequiredBalance(action Action) *big.Int {
	doubleMinimumBalance := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
	switch action {
	case NewAccountSend, ChangeSend:
		// In this account case, we must have keep a balance above
		// the minimum_balance in the sender's account and send
		// an amount of at least the minimum_balance to the recipient.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the recipient and the change address (or
		// we will create dust).
		return new(big.Int).Add(doubleMinimumBalance, t.maximumFee)
	case ExistingAccountSend, FullSend:
		// In the account case, we must keep a balance above
		// the minimum_balance in the sender's account.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the new UTXO.
		return new(big.Int).Add(t.minimumBalance, t.maximumFee)
	}

	return nil
}

// Balance returns the total balance to use for
// a transfer. In the case of a UTXO-based chain,
// this is the largest remaining UTXO.
func (t *ConstructionTester) Balance(
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

			if balance.Cmp(val) == -1 {
				balance = val
				coinIdentifier = coin.Identifier
			}
		}

		bal = balance
	default:
		// We should never hit this branch because the configuration file is
		// checked for issues like this before starting this loop.
		return nil, nil, fmt.Errorf(
			"invalid accounting model %s",
			t.config.Construction.AccountingModel,
		)
	}

	return bal, coinIdentifier, nil
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
		balance, coinIdentifier, err := t.Balance(ctx, address)
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

// FindSender fetches all available addresses,
// all locked addresses, and all address balances
// to determine which addresses can facilitate
// a transfer. The sender with the highest
// balance is returned (or the largest UTXO).
func (t *ConstructionTester) FindSender(
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

// FindRecipients either finds a random existing
// recipient or generates a new address.
func (t *ConstructionTester) FindRecipients(
	ctx context.Context,
	sender string,
) ([]string, error) {
	validRecipients := []string{}
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get address", err)
	}

	for _, a := range addresses {
		if a == sender {
			continue
		}

		validRecipients = append(validRecipients, a)
	}

	return validRecipients, nil
}

// CreateScenarioContext creates the context to use
// for scenario population.
func (t *ConstructionTester) CreateScenarioContext(
	ctx context.Context,
	sender string,
	senderValue *big.Int,
	recipient string,
	recipientValue *big.Int,
	changeAddress string,
	changeValue *big.Int,
	coinIdentifier *types.CoinIdentifier,
	scenarioOps []*types.Operation,
) (*scenario.Context, []*types.Operation, error) {
	if len(changeAddress) > 0 {
		scenarioOps = append(scenarioOps, t.config.Construction.ChangeIntent)
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

func (t *ConstructionTester) prettyAmount(amount *big.Int) string {
	nativeUnits := new(big.Float).SetInt(amount)
	divisor := utils.BigPow10(t.config.Construction.Currency.Decimals)
	nativeUnits = new(big.Float).Quo(nativeUnits, divisor)
	precision := int(t.config.Construction.Currency.Decimals)

	return fmt.Sprintf(
		"%s %s",
		nativeUnits.Text('f', precision),
		t.config.Construction.Currency.Symbol,
	)
}

// LogTransaction logs what a scenario is perfoming
// to the console.
func (t *ConstructionTester) LogTransaction(
	scenarioCtx *scenario.Context,
	transactionIdentifier *types.TransactionIdentifier,
) {
	if len(scenarioCtx.ChangeAddress) == 0 {
		color.Magenta(
			"Transaction Created: %s\n  %s -- %s --> %s",
			transactionIdentifier.Hash,
			scenarioCtx.Sender,
			t.prettyAmount(scenarioCtx.RecipientValue),
			scenarioCtx.Recipient,
		)
	} else {
		color.Magenta(
			"Transaction Created: %s\n  %s\n    -- %s --> %s\n    -- %s --> %s",
			transactionIdentifier.Hash,
			scenarioCtx.Sender,
			t.prettyAmount(scenarioCtx.RecipientValue),
			scenarioCtx.Recipient,
			t.prettyAmount(scenarioCtx.ChangeValue),
			scenarioCtx.ChangeAddress,
		)
	}
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
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", false, fmt.Errorf("%w: cannot create new address", err)
		}

		return addr, true, nil
	}

	return recipients[0], false, nil
}

func getRandomAmount(minimum *big.Int, lessThan *big.Int) *big.Int {
	source := rand.New(rand.NewSource(time.Now().Unix()))
	transformed := new(big.Int).Sub(lessThan, minimum)
	addition := new(big.Int).Rand(source, transformed)

	return new(big.Int).Add(minimum, addition)
}

func (t *ConstructionTester) getRecipientWithMinimum(
	ctx context.Context,
	recipients []string,
) (string, error) {
	for _, r := range recipients {
		bal, _, err := t.Balance(ctx, r)
		if err != nil {
			return "", fmt.Errorf("%w: unable to retrieve balance for %s", err, r)
		}

		if new(big.Int).Sub(bal, t.minimumBalance).Sign() >= 0 {
			return r, nil
		}
	}

	return "", nil
}

func (t *ConstructionTester) generateAccountIntent(
	ctx context.Context,
	balance *big.Int,
	recipients []string,
) (
	*big.Int, // Sender value
	string, // Recipient
	*big.Int, // Recipient Value
	string, // Change Address
	*big.Int, // Change Value
	error, // ErrInsufficientFunds
) {
	adjustedBalance := new(big.Int).Sub(balance, t.minimumBalance)

	// should send to new account, existing account, or no acccount?
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(NewAccountSend)).Sign() != -1 {
		recipient, created, err := t.canGetNewAddress(ctx, recipients)
		if err != nil {
			return nil, "", nil, "", nil, fmt.Errorf("%w: unable to get recipient", err)
		}

		if created {
			recipientValue := getRandomAmount(t.minimumBalance, adjustedBalance)
			return recipientValue, recipient, recipientValue, "", nil, nil
		}

		// If sending to an already existing account, can assume that balance
		// is greater than minimum_balance already.
		recipientValue := getRandomAmount(big.NewInt(0), adjustedBalance)
		return recipientValue, recipient, recipientValue, "", nil, nil
	}

	recipientValue := getRandomAmount(big.NewInt(0), adjustedBalance)
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(ExistingAccountSend)).Sign() != -1 {
		recipient, err := t.getRecipientWithMinimum(ctx, recipients)
		if err != nil {
			return nil, "", nil, "", nil, fmt.Errorf(
				"%w: unable to get recipient with minimum",
				err,
			)
		}

		if len(recipient) == 0 {
			return nil, "", nil, "", nil, ErrInsufficientFunds
		}

		return recipientValue, recipient, recipientValue, "", nil, nil
	}

	// Cannot perform any transfer.
	return nil, "", nil, "", nil, ErrInsufficientFunds
}

func (t *ConstructionTester) generateUTXOIntent(
	ctx context.Context,
	balance *big.Int,
	recipients []string,
) (
	*big.Int, // Sender value
	string, // Recipient
	*big.Int, // Recipient Value
	string, // Change Address
	*big.Int, // Change Value
	error, // ErrInsufficientFunds
) {
	feeLessBalance := new(big.Int).Sub(balance, t.maximumFee)
	recipient, created, err := t.canGetNewAddress(ctx, recipients)
	if err != nil {
		return nil, "", nil, "", nil, fmt.Errorf("%w: unable to get recipient", err)
	}

	// Need to remove from recipients if created a change address
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
	if new(big.Int).Sub(balance, t.minimumRequiredBalance(ChangeSend)).Sign() != -1 &&
		t.config.Construction.ChangeIntent != nil {
		changeAddress, _, err := t.canGetNewAddress(ctx, recipients)
		if err != nil {
			return nil, "", nil, "", nil, fmt.Errorf("%w: unable to get change address", err)
		}

		doubleMinimumBalance := new(big.Int).Add(t.minimumBalance, t.minimumBalance)
		changeDifferential := new(big.Int).Sub(feeLessBalance, doubleMinimumBalance)

		recipientShare := getRandomAmount(big.NewInt(0), changeDifferential)
		changeShare := new(big.Int).Sub(changeDifferential, recipientShare)

		recipientValue := new(big.Int).Add(t.minimumBalance, recipientShare)
		changeValue := new(big.Int).Add(t.minimumBalance, changeShare)

		return balance, recipient, recipientValue, changeAddress, changeValue, nil
	}

	if new(big.Int).Sub(balance, t.minimumRequiredBalance(FullSend)).Sign() != -1 {
		return balance, recipient, getRandomAmount(t.minimumBalance, feeLessBalance), "", nil, nil
	}

	// Cannot perform any transfer.
	return nil, "", nil, "", nil, ErrInsufficientFunds
}

func (t *ConstructionTester) GenerateBestIntent(
	ctx context.Context,
	balance *big.Int,
	recipients []string,
) (
	*big.Int, // Sender value
	string, // Recipient
	*big.Int, // Recipient Value
	string, // Change Address
	*big.Int, // Change Value
	error, // ErrInsufficientFunds
) {
	switch t.config.Construction.AccountingModel {
	case configuration.AccountModel:
		return t.generateAccountIntent(ctx, balance, recipients)
	case configuration.UtxoModel:
		return t.generateUTXOIntent(ctx, balance, recipients)
	}

	return nil, "", nil, "", nil, ErrInsufficientFunds
}

func (t *ConstructionTester) generateNewAndRequest(ctx context.Context) error {
	addr, err := t.NewAddress(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to create address", err)
	}

	_, _, err = t.RequestFunds(ctx, addr)
	if err != nil {
		return fmt.Errorf("%w: unable to get funds on %s", err, addr)
	}

	return nil
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
		sender, balance, coinIdentifier, err := t.FindSender(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		// Determine Action
		recipients, err := t.FindRecipients(ctx, sender)
		if err != nil {
			return fmt.Errorf("%w: unable to find recipient", err)
		}

		senderValue, recipient, recipientValue, changeAddress, changeValue, err := t.GenerateBestIntent(
			ctx,
			balance,
			recipients,
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

		scenarioCtx, scenarioOps, err := t.CreateScenarioContext(
			ctx,
			sender,
			senderValue,
			recipient,
			recipientValue,
			changeAddress,
			changeValue,
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
