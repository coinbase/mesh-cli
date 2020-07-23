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
	"github.com/coinbase/rosetta-cli/internal/processor"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	ErrBelowWaterMark = errors.New("below water mark")
)

type ConstructionTester struct {
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
	configuration  *configuration.ConstructionConfiguration
	highWaterMark  int64
	keyStorage     *storage.KeyStorage
	handler        *processor.CheckConstructionHandler
	startBlock     *types.BlockIdentifier
}

func New(
	ctx context.Context,
	config *configuration.ConstructionConfiguration,
	keyStorage *storage.KeyStorage,
) (*ConstructionTester, error) {
	t := &ConstructionTester{
		configuration: config,
		highWaterMark: -1,
		keyStorage:    keyStorage,
	}

	// Initialize Fetchers
	t.onlineFetcher = fetcher.New(t.configuration.OnlineURL)

	_, _, err := t.onlineFetcher.InitializeAsserter(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to initialize asserter", err)
	}

	networks, err := t.onlineFetcher.NetworkList(ctx, nil)
	networkMatched := false
	for _, network := range networks.NetworkIdentifiers {
		if types.Hash(network) == types.Hash(t.configuration.Network) {
			networkMatched = true
			break
		}
	}

	if !networkMatched {
		return nil, fmt.Errorf("%s is not available", types.PrettyPrintStruct(t.configuration.Network))
	}

	status, err := t.onlineFetcher.NetworkStatusRetry(
		ctx,
		t.configuration.Network,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get network status", err)
	}

	t.startBlock = status.CurrentBlockIdentifier

	t.offlineFetcher = fetcher.New(
		t.configuration.OfflineURL,
		fetcher.WithAsserter(t.onlineFetcher.Asserter), // use online asserter
	)

	// Load all accounts for network
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to load addresses", err)
	}

	log.Printf("construction tester initialized with %d addresses\n", len(addresses))

	return t, nil
}

func (t *Tester) ProduceTransaction(ctx context.Context, ops []*types.Operation) (*types.TransactionIdentifier, error) {
	log := ctxzap.Extract(ctx)
	log.Debug("created operations", zap.Any("operations", ops))

	metadataRequest, err := t.offlineFetcher.ConstructionPreprocess(
		ctx,
		t.configuration.Network,
		ops,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to preprocess", err)
	}
	log.Debug("created metadata request", zap.Any("request", metadataRequest))

	requiredMetadata, err := t.onlineFetcher.ConstructionMetadata(
		ctx,
		t.configuration.Network,
		metadataRequest,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to construct metadata", err)
	}
	log.Debug("recieved construction metadata", zap.Any("metadata", requiredMetadata))

	unsignedTransaction, payloads, err := t.offlineFetcher.ConstructionPayloads(
		ctx,
		t.configuration.Network,
		ops,
		requiredMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to construct payloads", err)
	}

	parsedOps, _, _, err := t.offlineFetcher.ConstructionParse(
		ctx,
		t.configuration.Network,
		false,
		unsignedTransaction,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to parse unsigned transaction", err)
	}
	log.Debug("parsed unsigned transaction", zap.Any("operations", parsedOps))

	if err := parser.OperationsIntent(ctx, ops, parsedOps); err != nil {
		return nil, fmt.Errorf("%w: unsigned parsed ops do not match intent", err)
	}

	requestedSigners := []string{}
	for _, payload := range payloads {
		requestedSigners = append(requestedSigners, payload.Address)
	}
	log.Debug("requested signers", zap.Any("signers", requestedSigners))

	signatures, err := t.keys.SignPayloads(payloads)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to sign payloads", err)
	}
	log.Debug("created signatures", zap.Any("signatures", signatures))

	networkTransaction, err := t.offlineFetcher.ConstructionCombine(
		ctx,
		t.configuration.Network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to combine signatures", err)
	}
	log.Debug("created network payload", zap.Any("payload", networkTransaction))

	signedParsedOps, signers, _, err := t.offlineFetcher.ConstructionParse(
		ctx,
		t.configuration.Network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to parse signed transaction", err)
	}
	log.Debug("parsed signed transaction", zap.Any("operations", signedParsedOps))
	log.Debug("parsed signers", zap.Any("signers", signers))

	if err := parser.OperationsIntent(ctx, ops, signedParsedOps); err != nil {
		return nil, fmt.Errorf("%w: signed parsed ops do not match intent", err)
	}

	if err := parser.SignersIntent(payloads, signers); err != nil {
		return nil, fmt.Errorf("%w: signed transactions signers do not match intent", err)
	}

	txHash, err := t.offlineFetcher.ConstructionHash(
		ctx,
		t.configuration.Network,
		networkTransaction,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get transaction hash", err)
	}

	// Submit Transaction
	txID, _, err := t.onlineFetcher.ConstructionSubmit(
		ctx,
		t.configuration.Network,
		networkTransaction,
	)
	if err != nil {
		return nil, fmt.Errorf("%w transaction submission failed", err)
	}
	log.Info("trasaction broadcast", zap.String("hash", txID.Hash))

	if txID.Hash != txHash {
		return nil, fmt.Errorf("derived transaction hash %s does not match hash returned by submit %s", txHash, txID.Hash)
	}

	// TODO: Look for TX in mempool (if enabled) and compare intent vs parsed ops
	// -> may need to differentiate between /mempool and /mempool/transaction support

	// Look at blocks and wait for tx
	var block *types.BlockIdentifier
	var chainTransaction *types.Transaction
	for ctx.Err() == nil {
		block, chainTransaction = t.transactionHandler.Transaction(ctx, txID)
		if block == nil || chainTransaction == nil {
			log.Debug("waiting for tx on chain", zap.String("hash", txID.Hash))

			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	log.Info("transaction found", zap.String("transaction hash", txHash), zap.Int64("block index", block.Index), zap.String("block hash", block.Hash))
	log.Debug("parsed on-chain transaction", zap.Any("operations", chainTransaction.Operations))

	if err := parser.OperationsIntent(ctx, ops, chainTransaction.Operations); err != nil {
		return nil, fmt.Errorf("%w: on-chain parsed ops do not match intent", err)
	}

	t.highWaterMark = block.Index
	return txID, nil
}

func (t *Tester) StartSyncer(
	ctx context.Context,
) error {
	trackUtxos := false
	if t.configuration.Model == configuration.Utxo {
		trackUtxos = true
	}

	t.transactionHandler = processor.NewTransactionHandler(
		ctx,
		trackUtxos,
	)

	syncer := syncer.New(
		t.configuration.Network,
		t.onlineFetcher,
		t.transactionHandler,
		nil,
		nil,
	)

	return syncer.Sync(ctx, t.startBlock.Index, -1)
}

func (t *Tester) NewAddress(
	ctx context.Context,
) (string, error) {
	return t.keys.AddKey(
		ctx,
		t.configuration.Network,
		t.onlineFetcher,
		t.configuration.CurveType,
	)
}

func (t *Tester) RequestLoad(
	ctx context.Context,
	address string,
) (*big.Int, error) {
	log := ctxzap.Extract(ctx)

	for {
		sendableBalance, err := t.SendableBalance(ctx, address)
		if err != nil {
			return nil, err
		}

		if sendableBalance != nil {
			log.Info("found sendable balance", zap.String("address", address), zap.String("balance", sendableBalance.String()))
			return sendableBalance, nil
		}

		log.Warn("waiting for funds", zap.String("address", address))
		time.Sleep(10 * time.Second)
	}
}

// returns spendable amount or error
func (t *Tester) SendableBalance(
	ctx context.Context,
	address string,
) (*big.Int, error) {
	var bal *big.Int
	if t.configuration.Model == configuration.Account {
		block, balances, _, err := t.onlineFetcher.AccountBalanceRetry(
			ctx,
			t.configuration.Network,
			&types.AccountIdentifier{
				Address: address,
			},
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to fetch balance for %s", err, address)
		}

		if block.Index < t.highWaterMark {
			return nil, ErrBelowWaterMark
		}

		var amount *types.Amount
		for _, bal := range balances {
			if types.Hash(bal.Currency) == types.Hash(t.configuration.Currency) {
				amount = bal
				break
			}
		}
		if amount == nil {
			return nil, errors.New("amount not found")
		}

		val, ok := new(big.Int).SetString(amount.Value, 10)
		if !ok {
			return nil, fmt.Errorf("could not parse amount for %s", address)
		}

		bal = val
	} else if t.configuration.Model == configuration.Utxo {
		balance, _, lastSynced, err := t.transactionHandler.GetUTXOBalance(address)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to get utxo balance for %s", err, address)
		}

		if lastSynced == -1 || lastSynced < t.highWaterMark {
			return nil, ErrBelowWaterMark
		}

		bal = balance
	} else {
		return nil, errors.New("invalid model")
	}

	sendableBalance := new(big.Int).Sub(bal, t.minimumAccountBalance)
	sendableBalance = new(big.Int).Sub(sendableBalance, t.maximumFee)

	if sendableBalance.Sign() != 1 {
		return nil, nil
	}

	return sendableBalance, nil
}

func (t *Tester) FindSender(
	ctx context.Context,
) (string, *big.Int, error) {
	if len(t.keys.Keys) == 0 { // create new and load
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", nil, fmt.Errorf("%w: unable to create address", err)
		}
		spendableBalance, err := t.RequestLoad(ctx, addr)
		if err != nil {
			return "", nil, fmt.Errorf("%w: unable to get load", err)
		}

		return addr, spendableBalance, nil
	}

	sendableAddresses := map[string]*big.Int{}
	for address, _ := range t.keys.Keys {
		sendableBalance, err := t.SendableBalance(ctx, address)
		if err != nil {
			return "", nil, err
		}

		if sendableBalance == nil {
			continue
		}

		sendableAddresses[address] = sendableBalance
	}

	if len(sendableAddresses) > 0 {
		addr := randomKey(sendableAddresses)

		return addr, sendableAddresses[addr], nil
	}

	// pick random to load up
	addr := randomKey(t.keys.Keys)
	sendableBalance, err := t.RequestLoad(ctx, addr)
	if err != nil {
		return "", nil, fmt.Errorf("%w: unable to get load", err)
	}

	return addr, sendableBalance, nil
}

func (t *Tester) FindRecipient(
	ctx context.Context,
	sender string,
) (string, error) {
	validRecipients := []string{}
	for k, _ := range t.keys.Keys {
		if k == sender {
			continue
		}

		validRecipients = append(validRecipients, k)
	}

	// Randomly generate new recipients
	coinFlip := false
	randomness := rand.New(rand.NewSource(time.Now().Unix())).Float64()
	if randomness > 0.5 {
		coinFlip = true
	}

	if len(validRecipients) == 0 || coinFlip {
		addr, err := t.NewAddress(ctx)
		if err != nil {
			return "", fmt.Errorf("%w: unable to generate new address", err)
		}

		return addr, nil
	}

	return validRecipients[rand.New(rand.NewSource(time.Now().Unix())).Intn(len(validRecipients))], nil
}

// must be map with string keys
func randomKey(m interface{}) string {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	keys := reflect.ValueOf(m).MapKeys()

	return keys[r.Intn(len(keys))].String()
}

// TODO: monitor mempool for deposits and transfers (useful for testing on blockchains with slow blocks like BTC)

func (t *Tester) TransferLoop(ctx context.Context) error {
	log := ctxzap.Extract(ctx)

	transactionsBroadcast := 0
	for ctx.Err() == nil {
		sender, sendableBalance, err := t.FindSender(ctx)
		if errors.Is(err, ErrBelowWaterMark) {
			log.Info("waiting for block to start next run", zap.Int64("block required", t.highWaterMark))

			time.Sleep(10 * time.Second)
			continue
		} else if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		recipient, err := t.FindRecipient(ctx, sender)
		if err != nil {
			return fmt.Errorf("%w: unable to find recipient", err)
		}

		var senderValue, recipientValue *big.Int
		var utxo *processor.UTXO
		if t.configuration.Model == configuration.Account {
			senderValue = new(big.Int).Rand(rand.New(rand.NewSource(time.Now().Unix())), sendableBalance)
			recipientValue = senderValue
		} else {
			_, utxos, _, err := t.transactionHandler.GetUTXOBalance(sender)
			if err != nil {
				return fmt.Errorf("%w: unable to get utxo balance for %s", err, sender)
			}

			utxo = utxos[0] // TODO: perform more complicated coin selection...right now it is FIFO
			senderValue, _ = new(big.Int).SetString(utxo.Operation.Amount.Value, 10)
			recipientValue = new(big.Int).Sub(senderValue, t.maximumFee)
		}

		// Populate Scenario
		scenarioContext := &scenarios.ScenarioContext{
			Sender:         sender,
			SenderValue:    senderValue,
			Recipient:      recipient,
			RecipientValue: recipientValue,
			UTXO:           utxo,
			Currency:       t.configuration.Currency,
		}

		ops, err := scenarios.PopulateScenario(ctx, scenarioContext, t.transferScenario)
		if err != nil {
			return fmt.Errorf("%w: unable to populate scenario", err)
		}

		// Perform and Recognize Transaction
		_, err = t.ProduceTransaction(ctx, ops)
		if err != nil {
			return fmt.Errorf("%w: unable to produce transaction with operations %s", err, types.PrettyPrintStruct(ops))
		}

		transactionsBroadcast++

		log.Info("transfer run completed",
			zap.Int("transactions broadcast", transactionsBroadcast),
			zap.Int("total accounts", len(t.keys.Keys)),
		)
	}

	return ctx.Err()
}
