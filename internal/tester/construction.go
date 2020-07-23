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
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
)

var (
	ErrBelowWaterMark = errors.New("below water mark")
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type ConstructionTester struct {
	configuration *configuration.ConstructionConfiguration
	logger        *logger.Logger
	onlineURL     string
	highWaterMark int64
	keyStorage    *storage.KeyStorage
	handler       *processor.CheckConstructionHandler
	startBlock    *types.BlockIdentifier

	// parsed configuration
	minimumBalance *big.Int
	maximumFee     *big.Int
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
}

func NewConstruction(
	ctx context.Context,
	config *configuration.Configuration,
	keyStorage *storage.KeyStorage,
	logger *logger.Logger,
) (*ConstructionTester, error) {
	t := &ConstructionTester{
		logger:        logger,
		configuration: config.Construction,
		onlineURL:     config.OnlineURL,
		highWaterMark: -1,
		keyStorage:    keyStorage,
	}

	minimumBalance, ok := new(big.Int).SetString(t.configuration.MinimumBalance, 10)
	if !ok {
		return nil, errors.New("cannot parse minimum balance")
	}
	t.minimumBalance = minimumBalance

	maximumFee, ok := new(big.Int).SetString(t.configuration.MaximumFee, 10)
	if !ok {
		return nil, errors.New("cannot parse maximum fee")
	}
	t.maximumFee = maximumFee

	// Initialize Fetchers
	t.onlineFetcher = fetcher.New(
		t.onlineURL,
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
	)

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
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
	)

	// Load all accounts for network
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to load addresses", err)
	}

	log.Printf("construction tester initialized with %d addresses\n", len(addresses))

	return t, nil
}

func (t *ConstructionTester) ProduceTransaction(ctx context.Context, ops []*types.Operation) (*types.TransactionIdentifier, error) {
	metadataRequest, err := t.offlineFetcher.ConstructionPreprocess(
		ctx,
		t.configuration.Network,
		ops,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to preprocess", err)
	}

	requiredMetadata, err := t.onlineFetcher.ConstructionMetadata(
		ctx,
		t.configuration.Network,
		metadataRequest,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to construct metadata", err)
	}

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

	if err := parser.ExpectedOperations(ops, parsedOps, false); err != nil {
		return nil, fmt.Errorf("%w: unsigned parsed ops do not match intent", err)
	}

	requestedSigners := []string{}
	for _, payload := range payloads {
		requestedSigners = append(requestedSigners, payload.Address)
	}

	signatures, err := t.keyStorage.Sign(ctx, payloads)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to sign payloads", err)
	}

	networkTransaction, err := t.offlineFetcher.ConstructionCombine(
		ctx,
		t.configuration.Network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to combine signatures", err)
	}

	signedParsedOps, signers, _, err := t.offlineFetcher.ConstructionParse(
		ctx,
		t.configuration.Network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to parse signed transaction", err)
	}

	if err := parser.ExpectedOperations(ops, signedParsedOps, false); err != nil {
		return nil, fmt.Errorf("%w: signed parsed ops do not match intent", err)
	}

	if err := parser.ExpectedSigners(payloads, signers); err != nil {
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
	log.Printf("trasaction broadcast %s\n", txID.Hash)

	if txID.Hash != txHash {
		return nil, fmt.Errorf("derived transaction hash %s does not match hash returned by submit %s", txHash, txID.Hash)
	}

	// TODO: Look for TX in mempool (if enabled) and compare intent vs parsed ops
	// -> may need to differentiate between /mempool and /mempool/transaction support

	// Look at blocks and wait for tx
	var block *types.BlockIdentifier
	var chainTransaction *types.Transaction
	for ctx.Err() == nil {
		block, chainTransaction = t.handler.Transaction(ctx, txID)
		if block == nil { // wait for transaction to appear on-chain
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	log.Printf("transaction found on-chain in block %s:%d\n", block.Hash, block.Index)

	if err := parser.ExpectedOperations(ops, chainTransaction.Operations, false); err != nil {
		return nil, fmt.Errorf("%w: on-chain parsed ops do not match intent", err)
	}

	t.highWaterMark = block.Index
	return txID, nil
}

func (t *ConstructionTester) StartSyncer(
	ctx context.Context,
	cancel context.CancelFunc,
) error {
	trackUtxos := false
	if t.configuration.AccountingModel == configuration.UtxoModel {
		trackUtxos = true
	}

	t.handler = processor.NewCheckConstructionHandler(
		ctx,
		trackUtxos,
	)

	syncer := syncer.New(
		t.configuration.Network,
		t.onlineFetcher,
		t.handler,
		cancel,
		nil,
	)

	return syncer.Sync(ctx, t.startBlock.Index, -1)
}

func (t *ConstructionTester) NewAddress(
	ctx context.Context,
) (string, error) {
	kp, err := keys.GenerateKeypair(t.configuration.CurveType)
	if err != nil {
		return "", fmt.Errorf("%w unable to generate keypair", err)
	}

	address, _, err := t.offlineFetcher.ConstructionDerive(
		ctx,
		t.configuration.Network,
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

	_, _ = t.logger.CounterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return address, nil
}

func (t *ConstructionTester) RequestLoad(
	ctx context.Context,
	address string,
) (*big.Int, error) {
	printedMessage := false
	for ctx.Err() == nil {
		sendableBalance, err := t.SendableBalance(ctx, address)
		if err != nil {
			return nil, err
		}

		if sendableBalance != nil {
			log.Printf("found sendable balance %s on %s\n", sendableBalance.String(), address)
			return sendableBalance, nil
		}

		if !printedMessage {
			color.Yellow("waiting for funds on %s", address)
			printedMessage = true
		}
		time.Sleep(10 * time.Second)
	}

	return nil, ctx.Err()
}

// returns spendable amount or error
func (t *ConstructionTester) SendableBalance(
	ctx context.Context,
	address string,
) (*big.Int, error) {
	var bal *big.Int
	if t.configuration.AccountingModel == configuration.AccountModel {
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
	} else {
		balance, _, lastSynced, err := t.handler.GetUTXOBalance(address)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to get utxo balance for %s", err, address)
		}

		if lastSynced == -1 || lastSynced < t.highWaterMark {
			return nil, ErrBelowWaterMark
		}

		bal = balance
	}

	sendableBalance := new(big.Int).Sub(bal, t.minimumBalance)
	sendableBalance = new(big.Int).Sub(sendableBalance, t.maximumFee)

	if sendableBalance.Sign() != 1 {
		return nil, nil
	}

	return sendableBalance, nil
}

func (t *ConstructionTester) FindSender(
	ctx context.Context,
) (string, *big.Int, error) {
	addresses, err := t.keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("%w: unable to get addresses", err)
	}

	if len(addresses) == 0 { // create new and load
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
	for _, address := range addresses {
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
		sendableKeys := reflect.ValueOf(sendableAddresses).MapKeys()
		addr := sendableKeys[rand.Intn(len(sendableKeys))].String()

		return addr, sendableAddresses[addr], nil
	}

	// pick random to load up
	addr, err := t.keyStorage.RandomAddress(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("%w: unable to get random address", err)
	}

	sendableBalance, err := t.RequestLoad(ctx, addr)
	if err != nil {
		return "", nil, fmt.Errorf("%w: unable to get load", err)
	}

	return addr, sendableBalance, nil
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

// TODO: monitor mempool for deposits and transfers (useful for testing on blockchains with slow blocks like BTC)

func (t *ConstructionTester) TransferLoop(ctx context.Context) error {
	for ctx.Err() == nil {
		sender, sendableBalance, err := t.FindSender(ctx)
		if errors.Is(err, ErrBelowWaterMark) { // wait until above high water mark to start next loop
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
		if t.configuration.AccountingModel == configuration.AccountModel {
			senderValue = new(big.Int).Rand(rand.New(rand.NewSource(time.Now().Unix())), sendableBalance)
			recipientValue = senderValue
		} else {
			_, utxos, _, err := t.handler.GetUTXOBalance(sender)
			if err != nil {
				return fmt.Errorf("%w: unable to get utxo balance for %s", err, sender)
			}

			utxo = utxos[0] // TODO: perform more complicated coin selection...right now it is FIFO
			senderValue, _ = new(big.Int).SetString(utxo.Operation.Amount.Value, 10)
			recipientValue = new(big.Int).Sub(senderValue, t.maximumFee) // TODO: send less than max fee and provide change address
		}

		// Populate Scenario
		scenarioContext := &scenario.Context{
			Sender:         sender,
			SenderValue:    senderValue,
			Recipient:      recipient,
			RecipientValue: recipientValue,
			Currency:       t.configuration.Currency,
		}
		if utxo != nil {
			scenarioContext.UTXOIdentifier = utxo.Identifier
		}

		ops, err := scenario.PopulateScenario(ctx, scenarioContext, t.configuration.TransferScenario)
		if err != nil {
			return fmt.Errorf("%w: unable to populate scenario", err)
		}

		// Perform and Recognize Transaction
		_, err = t.ProduceTransaction(ctx, ops)
		if err != nil {
			return fmt.Errorf("%w: unable to produce transaction with operations %s", err, types.PrettyPrintStruct(ops))
		}

		_, _ = t.logger.CounterStorage.Update(ctx, storage.TransactionsCreatedCounter, big.NewInt(1))
	}

	return ctx.Err()
}
