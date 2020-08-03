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

package constructor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/slowboat0/rosetta-cli/pkg/logger"
	"github.com/slowboat0/rosetta-cli/pkg/scenario"
	"github.com/slowboat0/rosetta-cli/pkg/storage"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"github.com/jinzhu/copier"
)

// action is some supported intent we can
// perform. This is used to determine the minimum
// required balance to complete the transaction.
type action string

const (
	convertToInt = 100

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

// Helper is an interface that provides access
// to information needed by the constructor while
// creating transactions.
type Helper interface {
	// Derive returns a new address for a provided publicKey.
	Derive(
		context.Context,
		*types.NetworkIdentifier,
		*types.PublicKey,
		map[string]interface{},
	) (string, map[string]interface{}, error)

	// Preprocess calls the /construction/preprocess endpoint
	// on an offline node.
	Preprocess(
		context.Context,
		*types.NetworkIdentifier,
		[]*types.Operation,
		map[string]interface{},
	) (map[string]interface{}, error)

	// Metadata calls the /construction/metadata endpoint
	// using the online node.
	Metadata(
		context.Context,
		*types.NetworkIdentifier,
		map[string]interface{},
	) (map[string]interface{}, error)

	// Payloads calls the /construction/payloads endpoint
	// using the offline node.
	Payloads(
		context.Context,
		*types.NetworkIdentifier,
		[]*types.Operation,
		map[string]interface{},
	) (string, []*types.SigningPayload, error)

	// Parse calls the /construction/parse endpoint
	// using the offline node.
	Parse(
		context.Context,
		*types.NetworkIdentifier,
		bool, // signed
		string, // transaction
	) ([]*types.Operation, []string, map[string]interface{}, error)

	// Combine calls the /construction/combine endpoint
	// using the offline node.
	Combine(
		context.Context,
		*types.NetworkIdentifier,
		string, // unsigned transaction
		[]*types.Signature,
	) (string, error)

	// Broadcast enqueues a particular intent for broadcast.
	Broadcast(
		context.Context,
		string, // sender
		[]*types.Operation, // intent
		*types.TransactionIdentifier,
		string, // payload
	) error

	// Hash calls the /construction/hash endpoint
	// using the offline node.
	Hash(
		context.Context,
		*types.NetworkIdentifier,
		string, // network transaction
	) (*types.TransactionIdentifier, error)

	// Sign returns signatures for the provided
	// payloads.
	Sign(
		context.Context,
		[]*types.SigningPayload,
	) ([]*types.Signature, error)

	// StoreKey is called to persist an
	// address + KeyPair.
	StoreKey(
		context.Context,
		string,
		*keys.KeyPair,
	) error

	// AccountBalance returns the balance
	// for a provided address.
	AccountBalance(
		context.Context,
		*types.AccountIdentifier,
		*types.Currency,
	) (*big.Int, error)

	// CoinBalance returns the balance of the largest
	// Coin owned by an address.
	CoinBalance(
		context.Context,
		*types.AccountIdentifier,
		*types.Currency,
	) (*big.Int, *types.CoinIdentifier, error)

	// LockedAddresses is a slice of all addresses currently sending or receiving
	// funds.
	LockedAddresses(context.Context) ([]string, error)

	// AllBroadcasts returns a slice of all in-progress broadcasts.
	AllBroadcasts(ctx context.Context) ([]*storage.Broadcast, error)

	// ClearBroadcasts deletes all pending broadcasts.
	ClearBroadcasts(ctx context.Context) ([]*storage.Broadcast, error)

	// AllAddresses returns a slice of all known addresses.
	AllAddresses(ctx context.Context) ([]string, error)

	// RandomAmount returns some integer between min and max.
	RandomAmount(*big.Int, *big.Int) *big.Int
}

// Handler is an interface called by the constructor whenever
// an address is created or a transaction is created.
type Handler interface {
	AddressCreated(context.Context, string) error
	TransactionCreated(context.Context, string, *types.TransactionIdentifier) error
}

// Constructor is responsible for managing the entire flow
// of creating test transactions on the Construction API.
// This runs from creating keypairs to requesting funds be
// loaded on addresses to creating transactions that satisfy
// minimum balance constraints.
type Constructor struct {
	network               *types.NetworkIdentifier
	accountingModel       configuration.AccountingModel
	currency              *types.Currency
	minimumBalance        *big.Int
	maximumFee            *big.Int
	curveType             types.CurveType
	newAccountProbability float64
	maxAddresses          int

	scenario       []*types.Operation
	changeScenario *types.Operation

	parser  *parser.Parser
	helper  Helper
	handler Handler
}

// New returns a new *Constructor.
func New(
	config *configuration.Configuration,
	parser *parser.Parser,
	helper Helper,
	handler Handler,
) (*Constructor, error) {
	minimumBalance, ok := new(big.Int).SetString(config.Construction.MinimumBalance, 10)
	if !ok {
		return nil, errors.New("cannot parse minimum balance")
	}

	maximumFee, ok := new(big.Int).SetString(config.Construction.MaximumFee, 10)
	if !ok {
		return nil, errors.New("cannot parse maximum fee")
	}

	return &Constructor{
		network:               config.Network,
		accountingModel:       config.Construction.AccountingModel,
		currency:              config.Construction.Currency,
		minimumBalance:        minimumBalance,
		maximumFee:            maximumFee,
		curveType:             config.Construction.CurveType,
		newAccountProbability: config.Construction.NewAccountProbability,
		maxAddresses:          config.Construction.MaxAddresses,
		scenario:              config.Construction.Scenario,
		changeScenario:        config.Construction.ChangeScenario,
		helper:                helper,
		handler:               handler,
	}, nil
}

// createTransaction constructs and signs a transaction with the provided intent.
func (c *Constructor) createTransaction(
	ctx context.Context,
	intent []*types.Operation,
) (*types.TransactionIdentifier, string, error) {
	metadataRequest, err := c.helper.Preprocess(
		ctx,
		c.network,
		intent,
		nil,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to preprocess", err)
	}

	requiredMetadata, err := c.helper.Metadata(
		ctx,
		c.network,
		metadataRequest,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct metadata", err)
	}

	unsignedTransaction, payloads, err := c.helper.Payloads(
		ctx,
		c.network,
		intent,
		requiredMetadata,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct payloads", err)
	}

	parsedOps, signers, _, err := c.helper.Parse(
		ctx,
		c.network,
		false,
		unsignedTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse unsigned transaction", err)
	}

	if len(signers) != 0 {
		return nil, "", fmt.Errorf(
			"signers should be empty in unsigned transaction but found %d",
			len(signers),
		)
	}

	if err := c.parser.ExpectedOperations(intent, parsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: unsigned parsed ops do not match intent", err)
	}

	signatures, err := c.helper.Sign(ctx, payloads)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to sign payloads", err)
	}

	networkTransaction, err := c.helper.Combine(
		ctx,
		c.network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to combine signatures", err)
	}

	signedParsedOps, signers, _, err := c.helper.Parse(
		ctx,
		c.network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse signed transaction", err)
	}

	if err := c.parser.ExpectedOperations(intent, signedParsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: signed parsed ops do not match intent", err)
	}

	if err := parser.ExpectedSigners(payloads, signers); err != nil {
		return nil, "", fmt.Errorf("%w: signed transactions signers do not match intent", err)
	}

	transactionIdentifier, err := c.helper.Hash(
		ctx,
		c.network,
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
func (c *Constructor) newAddress(ctx context.Context) (string, error) {
	kp, err := keys.GenerateKeypair(c.curveType)
	if err != nil {
		return "", fmt.Errorf("%w unable to generate keypair", err)
	}

	address, _, err := c.helper.Derive(
		ctx,
		c.network,
		kp.PublicKey,
		nil,
	)

	if err != nil {
		return "", fmt.Errorf("%w: unable to derive address", err)
	}

	err = c.helper.StoreKey(ctx, address, kp)
	if err != nil {
		return "", fmt.Errorf("%w: unable to store address", err)
	}

	if err := c.handler.AddressCreated(ctx, address); err != nil {
		return "", fmt.Errorf("%w: could not handle address creation", err)
	}

	return address, nil
}

// requestFunds prompts the user to load
// a particular address with funds from a faucet.
// TODO: automate this using an API faucet.
func (c *Constructor) requestFunds(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	var lastMessage string
	for ctx.Err() == nil {
		balance, coinIdentifier, err := c.balance(ctx, address)
		if err != nil {
			return nil, nil, err
		}

		minBalance := c.minimumRequiredBalance(newAccountSend)
		if c.accountingModel == configuration.UtxoModel {
			minBalance = c.minimumRequiredBalance(changeSend)
		}

		if balance != nil && new(big.Int).Sub(balance, minBalance).Sign() != -1 {
			color.Green("Found balance %s on %s", utils.PrettyAmount(balance, c.currency), address)
			return balance, coinIdentifier, nil
		}

		message := fmt.Sprintf("Waiting for funds on %s", address)
		if balance != nil && balance.Sign() == 1 {
			message = fmt.Sprintf(
				"Found balance %s on %s (need %s to continue)",
				utils.PrettyAmount(balance, c.currency),
				address,
				utils.PrettyAmount(minBalance, c.currency),
			)
		}

		if message != lastMessage {
			color.Yellow(message)
			lastMessage = message
		}

		time.Sleep(defaultSleepTime * time.Second)
	}

	return nil, nil, ctx.Err()
}

func (c *Constructor) minimumRequiredBalance(action action) *big.Int {
	doubleMinimumBalance := new(big.Int).Add(c.minimumBalance, c.minimumBalance)
	switch action {
	case newAccountSend, changeSend:
		// In this account case, we must have keep a balance above
		// the minimum_balance in the sender's account and send
		// an amount of at least the minimum_balance to the recipient.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the recipient and the change address (or
		// we will create dust).
		return new(big.Int).Add(doubleMinimumBalance, c.maximumFee)
	case existingAccountSend, fullSend:
		// In the account case, we must keep a balance above
		// the minimum_balance in the sender's account.
		//
		// In the UTXO case, we must send at least the minimum
		// balance to the new UTXO.
		return new(big.Int).Add(c.minimumBalance, c.maximumFee)
	}

	return nil
}

// balance returns the total balance to use for
// a transfer. In the case of a UTXO-based chain,
// this is the largest remaining UTXO.
func (c *Constructor) balance(
	ctx context.Context,
	address string,
) (*big.Int, *types.CoinIdentifier, error) {
	accountIdentifier := &types.AccountIdentifier{Address: address}

	switch c.accountingModel {
	case configuration.AccountModel:
		bal, err := c.helper.AccountBalance(ctx, accountIdentifier, c.currency)

		return bal, nil, err
	case configuration.UtxoModel:
		return c.helper.CoinBalance(ctx, accountIdentifier, c.currency)
	}

	return nil, nil, fmt.Errorf("unable to find balance for %s", address)
}

func (c *Constructor) bestUnlockedSender(
	ctx context.Context,
	addresses []string,
) (
	string, // best address
	*big.Int, // best balance
	*types.CoinIdentifier, // best coin
	error,
) {
	unlockedAddresses := []string{}
	lockedAddresses, err := c.helper.LockedAddresses(ctx)
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
		balance, coinIdentifier, err := c.balance(ctx, address)
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
func (c *Constructor) findSender(
	ctx context.Context,
) (
	string, // sender
	*big.Int, // balance
	*types.CoinIdentifier, // coin
	error,
) {
	for ctx.Err() == nil {
		addresses, err := c.helper.AllAddresses(ctx)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get addresses", err)
		}

		if len(addresses) == 0 { // create new and load
			err := c.generateNewAndRequest(ctx)
			if err != nil {
				return "", nil, nil, fmt.Errorf("%w: unable to generate new and request", err)
			}

			continue // we will exit on next loop
		}

		bestAddress, bestBalance, bestCoin, err := c.bestUnlockedSender(ctx, addresses)
		if err != nil {
			return "", nil, nil, fmt.Errorf("%w: unable to get best unlocked sender", err)
		}

		if len(bestAddress) > 0 {
			return bestAddress, bestBalance, bestCoin, nil
		}

		broadcasts, err := c.helper.AllBroadcasts(ctx)
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

		if err := c.generateNewAndRequest(ctx); err != nil {
			return "", nil, nil, fmt.Errorf("%w: generate new address and request", err)
		}
	}

	return "", nil, nil, ctx.Err()
}

// findRecipients returns all possible
// recipients (address != sender).
func (c *Constructor) findRecipients(
	ctx context.Context,
	sender string,
) (
	[]string, // recipients with minimum balance
	[]string, // recipients without minimum balance
	error,
) {
	minimumRecipients := []string{}
	belowMinimumRecipients := []string{}

	addresses, err := c.helper.AllAddresses(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get address", err)
	}
	for _, a := range addresses {
		if a == sender {
			continue
		}

		// Sending UTXOs always requires sending to the minimum.
		if c.accountingModel == configuration.UtxoModel {
			belowMinimumRecipients = append(belowMinimumRecipients, a)

			continue
		}

		bal, _, err := c.balance(ctx, a)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to retrieve balance for %s", err, a)
		}

		if new(big.Int).Sub(bal, c.minimumBalance).Sign() >= 0 {
			minimumRecipients = append(minimumRecipients, a)

			continue
		}

		belowMinimumRecipients = append(belowMinimumRecipients, a)
	}

	return minimumRecipients, belowMinimumRecipients, nil
}

// createScenarioContext creates the context to use
// for scenario population.
func (c *Constructor) createScenarioContext(
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
	if err := copier.Copy(&scenarioOps, c.scenario); err != nil {
		return nil, nil, fmt.Errorf("%w: unable to copy scenario", err)
	}

	if len(changeAddress) > 0 {
		changeCopy := types.Operation{}
		if err := copier.Copy(&changeCopy, c.changeScenario); err != nil {
			return nil, nil, fmt.Errorf("%w: unable to copy change intent", err)
		}

		scenarioOps = append(scenarioOps, &changeCopy)
	}

	return &scenario.Context{
		Sender:         sender,
		SenderValue:    senderValue,
		Recipient:      recipient,
		RecipientValue: recipientValue,
		Currency:       c.currency,
		CoinIdentifier: coinIdentifier,
		ChangeAddress:  changeAddress,
		ChangeValue:    changeValue,
	}, scenarioOps, nil
}

func (c *Constructor) canGetNewAddress(
	ctx context.Context,
	recipients []string,
) (string, bool, error) {
	addresses, err := c.helper.AllAddresses(ctx)
	if err != nil {
		return "", false, fmt.Errorf("%w: unable to get available addresses", err)
	}

	if len(addresses) >= c.maxAddresses && len(recipients) > 0 {
		return recipients[0], false, nil
	}

	randNumber := c.helper.RandomAmount(utils.ZeroInt, utils.OneHundredInt).Int64()
	convertedAccountProbability := int64(c.newAccountProbability * convertToInt)
	if randNumber < convertedAccountProbability || len(recipients) == 0 {
		addr, err := c.newAddress(ctx)
		if err != nil {
			return "", false, fmt.Errorf("%w: cannot create new address", err)
		}

		return addr, true, nil
	}

	return recipients[0], false, nil
}

func (c *Constructor) generateAccountScenario(
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
	sendableBalance := new(big.Int).Sub(balance, c.minimumBalance)
	sendableBalance = new(big.Int).Sub(sendableBalance, c.maximumFee)

	// should send to new account, existing account, or no acccount?
	if new(big.Int).Sub(balance, c.minimumRequiredBalance(newAccountSend)).Sign() != -1 {
		recipient, created, err := c.canGetNewAddress(
			ctx,
			append(minimumRecipients, belowMinimumRecipients...),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get recipient", err)
		}

		if created || utils.ContainsString(belowMinimumRecipients, recipient) {
			recipientValue := c.helper.RandomAmount(c.minimumBalance, sendableBalance)
			return c.createScenarioContext(
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
		recipientValue := c.helper.RandomAmount(big.NewInt(0), sendableBalance)
		return c.createScenarioContext(
			sender,
			recipientValue,
			recipient,
			recipientValue,
			"",
			nil,
			nil,
		)
	}

	if new(big.Int).Sub(balance, c.minimumRequiredBalance(existingAccountSend)).Sign() != -1 {
		if len(minimumRecipients) == 0 {
			return nil, nil, ErrInsufficientFunds
		}

		recipientValue := c.helper.RandomAmount(big.NewInt(0), sendableBalance)
		return c.createScenarioContext(
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

func (c *Constructor) generateUtxoScenario(
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
	feeLessBalance := new(big.Int).Sub(balance, c.maximumFee)
	recipient, created, err := c.canGetNewAddress(ctx, recipients)
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
	if new(big.Int).Sub(balance, c.minimumRequiredBalance(changeSend)).Sign() != -1 &&
		c.changeScenario != nil {
		changeAddress, _, err := c.canGetNewAddress(ctx, recipients)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get change address", err)
		}

		doubleMinimumBalance := new(big.Int).Add(c.minimumBalance, c.minimumBalance)
		changeDifferential := new(big.Int).Sub(feeLessBalance, doubleMinimumBalance)

		recipientShare := c.helper.RandomAmount(big.NewInt(0), changeDifferential)
		changeShare := new(big.Int).Sub(changeDifferential, recipientShare)

		recipientValue := new(big.Int).Add(c.minimumBalance, recipientShare)
		changeValue := new(big.Int).Add(c.minimumBalance, changeShare)

		return c.createScenarioContext(
			sender,
			balance,
			recipient,
			recipientValue,
			changeAddress,
			changeValue,
			coinIdentifier,
		)
	}

	if new(big.Int).Sub(balance, c.minimumRequiredBalance(fullSend)).Sign() != -1 {
		return c.createScenarioContext(
			sender,
			balance,
			recipient,
			feeLessBalance,
			"",
			nil,
			coinIdentifier,
		)
	}

	// Cannot perform any transfer.
	return nil, nil, ErrInsufficientFunds
}

// generateScenario determines what should be done in a given
// transfer based on the sender's balance.
func (c *Constructor) generateScenario(
	ctx context.Context,
	sender string,
	balance *big.Int,
	coinIdentifier *types.CoinIdentifier,
) (
	*scenario.Context,
	[]*types.Operation, // scenario operations
	error, // ErrInsufficientFunds
) {
	if balance.Sign() == 0 {
		// Cannot perform any transfer.
		return nil, nil, ErrInsufficientFunds
	}

	minimumRecipients, belowMinimumRecipients, err := c.findRecipients(ctx, sender)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to find recipients", err)
	}

	switch c.accountingModel {
	case configuration.AccountModel:
		return c.generateAccountScenario(
			ctx,
			sender,
			balance,
			minimumRecipients,
			belowMinimumRecipients,
		)
	case configuration.UtxoModel:
		return c.generateUtxoScenario(ctx, sender, balance, belowMinimumRecipients, coinIdentifier)
	}

	return nil, nil, ErrInsufficientFunds
}

func (c *Constructor) generateNewAndRequest(ctx context.Context) error {
	addr, err := c.newAddress(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to create address", err)
	}

	_, _, err = c.requestFunds(ctx, addr)
	if err != nil {
		return fmt.Errorf("%w: unable to get funds on %s", err, addr)
	}

	return nil
}

// CreateTransactions loops on the create transaction loop
// until the caller cancels the context.
func (c *Constructor) CreateTransactions(
	ctx context.Context,
	clearBroadcasts bool,
) error {
	// Before starting loop, delete any pending broadcasts if configuration
	// indicates to do so.
	if clearBroadcasts {
		broadcasts, err := c.helper.ClearBroadcasts(ctx)
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
		sender, balance, coinIdentifier, err := c.findSender(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to find sender", err)
		}

		// Determine Action
		scenarioCtx, scenarioOps, err := c.generateScenario(
			ctx,
			sender,
			balance,
			coinIdentifier,
		)
		if errors.Is(err, ErrInsufficientFunds) {
			broadcasts, err := c.helper.AllBroadcasts(ctx)
			if err != nil {
				return fmt.Errorf("%w: unable to get broadcasts", err)
			}

			if len(broadcasts) > 0 {
				// we will wait for in-flight to process
				time.Sleep(defaultSleepTime * time.Second)
				continue
			}

			if err := c.generateNewAndRequest(ctx); err != nil {
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
		transactionIdentifier, networkTransaction, err := c.createTransaction(ctx, intent)
		if err != nil {
			return fmt.Errorf(
				"%w: unable to create transaction with operations %s",
				err,
				types.PrettyPrintStruct(intent),
			)
		}

		logger.LogScenario(scenarioCtx, transactionIdentifier, c.currency)

		// Broadcast Transaction
		err = c.helper.Broadcast(
			ctx,
			sender,
			intent,
			transactionIdentifier,
			networkTransaction,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to enqueue transaction for broadcast", err)
		}

		if err := c.handler.TransactionCreated(ctx, sender, transactionIdentifier); err != nil {
			return fmt.Errorf("%w: unable to handle transaction creation", err)
		}
	}

	return ctx.Err()
}
