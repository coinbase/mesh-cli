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
)

// ConstructionTester coordinates the `check:construction` test.
type ConstructionTester struct {
	network          *types.NetworkIdentifier
	database         storage.Database
	config           *configuration.Configuration
	syncer           *statefulsyncer.StatefulSyncer
	logger           *logger.Logger
	counterStorage   *storage.CounterStorage
	keyStorage       *storage.KeyStorage
	broadcastStorage *storage.BroadcastStorage
	blockStorage     *storage.BlockStorage
	coinStorage      *storage.CoinStorage
	parser           *parser.Parser

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
		_ = t.logger.LogConstructionStats(ctx)
		time.Sleep(PeriodicLoggingFrequency)
	}

	// Print stats one last time before exiting
	_ = t.logger.LogConstructionStats(ctx)

	return ctx.Err()
}

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
	blockStorage := storage.NewBlockStorage(localStore)
	keyStorage := storage.NewKeyStorage(localStore)
	coinStorage := storage.NewCoinStorage(localStore, onlineFetcher.Asserter)
	broadcastStorage := storage.NewBroadcastStorage(
		localStore,
		config.Construction.ConfirmationDepth,
		config.Construction.StaleDepth,
		config.Construction.BroadcastLimit,
		config.Construction.BroadcastTrailLimit,
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

	logger := logger.NewLogger(
		counterStorage,
		dataPath,
		false,
		false,
		false,
		false,
	)

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

	syncer := statefulsyncer.New(
		ctx,
		network,
		onlineFetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]storage.BlockWorker{coinStorage, broadcastStorage},
	)

	return &ConstructionTester{
		network:          network,
		database:         localStore,
		config:           config,
		syncer:           syncer,
		logger:           logger,
		counterStorage:   counterStorage,
		keyStorage:       keyStorage,
		broadcastStorage: broadcastStorage,
		blockStorage:     blockStorage,
		coinStorage:      coinStorage,
		parser:           parser,
		minimumBalance:   minimumBalance,
		maximumFee:       maximumFee,
		onlineFetcher:    onlineFetcher,
		offlineFetcher:   offlineFetcher,
	}, nil
}

func (t *ConstructionTester) CreateTransaction(ctx context.Context, intent []*types.Operation) (*types.TransactionIdentifier, string, error) {
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

	requestedSigners := []string{}
	for _, payload := range payloads {
		requestedSigners = append(requestedSigners, payload.Address)
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

	_, _ = t.counterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return address, nil
}

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
		time.Sleep(10 * time.Second)
	}

	return nil, nil, ctx.Err()
}

// returns spendable amount or error
func (t *ConstructionTester) SendableBalance(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	accountIdentifier := &types.AccountIdentifier{Address: address}
	var bal *big.Int
	var coinIdentifier *types.CoinIdentifier
	if t.config.Construction.AccountingModel == configuration.AccountModel {
		_, balances, _, _, err := t.onlineFetcher.AccountBalanceRetry(
			ctx,
			t.network,
			accountIdentifier,
			nil,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to fetch balance for %s", err, address)
		}

		var amount *types.Amount
		for _, bal := range balances {
			if types.Hash(bal.Currency) == types.Hash(t.config.Construction.Currency) {
				amount = bal
				break
			}
		}
		if amount == nil {
			return nil, nil, errors.New("amount not found")
		}

		val, ok := new(big.Int).SetString(amount.Value, 10)
		if !ok {
			return nil, nil, fmt.Errorf("could not parse amount for %s", address)
		}

		bal = val
	} else {
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
	}

	sendableBalance := new(big.Int).Sub(bal, t.minimumBalance)
	sendableBalance = new(big.Int).Sub(sendableBalance, t.maximumFee)

	if sendableBalance.Sign() != 1 {
		return nil, nil, nil
	}

	return sendableBalance, coinIdentifier, nil
}

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

func (t *ConstructionTester) FindRecipient(
	ctx context.Context,
	sender string,
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
	if rand.Float64() > 0.5 {
		coinFlip = true
	}

	if len(validRecipients) == 0 || coinFlip {
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate new address", err)
		}

		return addr, nil
	}

	return validRecipients[rand.Intn(len(validRecipients))], nil
}

func (t *ConstructionTester) CreateTransactions(ctx context.Context) error {
	for ctx.Err() == nil {
		sender, sendableBalance, coinIdentifier, err := t.FindSender(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		if len(sender) == 0 {
			// This condition occurs when we are waiting for some
			// pending broadcast to complete before creating more
			// transactions.

			time.Sleep(10 * time.Second)
			continue
		}

		recipient, err := t.FindRecipient(ctx, sender)
		if err != nil {
			return fmt.Errorf("%w: unable to find recipient", err)
		}

		senderValue := new(big.Int).Rand(rand.New(rand.NewSource(time.Now().Unix())), sendableBalance)
		recipientValue := senderValue
		if t.config.Construction.AccountingModel == configuration.UtxoModel {
			// TODO: send less than max fee and provide change address
			recipientValue = new(big.Int).Sub(senderValue, t.maximumFee)
		}

		// Populate Scenario
		scenarioContext := &scenario.Context{
			Sender:         sender,
			SenderValue:    senderValue,
			Recipient:      recipient,
			RecipientValue: recipientValue,
			Currency:       t.config.Construction.Currency,
			CoinIdentifier: coinIdentifier,
		}

		intent, err := scenario.PopulateScenario(ctx, scenarioContext, t.config.Construction.TransferScenario)
		if err != nil {
			return fmt.Errorf("%w: unable to populate scenario", err)
		}

		// Create transaction
		transactionIdentifier, networkTransaction, err := t.CreateTransaction(ctx, intent)

		nativeUnits := new(big.Float).SetInt(recipientValue)
		divisor := utils.BigPow10(t.config.Construction.Currency.Decimals)
		nativeUnits = new(big.Float).Quo(nativeUnits, divisor)
		color.Magenta(
			"%s -- %s%s --> %s Hash:%s",
			sender,
			nativeUnits.String(),
			t.config.Construction.Currency.Symbol,
			recipient,
			transactionIdentifier.Hash,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to create transaction with operations %s", err, types.PrettyPrintStruct(intent))
		}

		// Broadcast Transaction
		err = t.broadcastStorage.Broadcast(ctx, sender, intent, transactionIdentifier, networkTransaction)
		if err != nil {
			return fmt.Errorf("%w: unable to enqueue transaction for broadcast", err)
		}

		_, _ = t.logger.CounterStorage.Update(ctx, storage.TransactionsCreatedCounter, big.NewInt(1))
	}

	return ctx.Err()
}
