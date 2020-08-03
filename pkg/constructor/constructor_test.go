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
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/slowboat0/rosetta-cli/pkg/scenario"
	"github.com/slowboat0/rosetta-cli/pkg/storage"
	mocks "github.com/coinbase/rosetta-cli/mocks/constructor"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func simpleAsserterConfiguration() (*asserter.Asserter, error) {
	return asserter.NewClientWithOptions(
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		&types.BlockIdentifier{
			Hash:  "block 0",
			Index: 0,
		},
		[]string{"Transfer"},
		[]*types.OperationStatus{
			{
				Status:     "success",
				Successful: true,
			},
			{
				Status:     "failure",
				Successful: false,
			},
		},
		[]*types.Error{},
	)
}

func defaultParser(t *testing.T) *parser.Parser {
	asserter, err := simpleAsserterConfiguration()
	assert.NoError(t, err)

	return parser.New(asserter, nil)
}

func defaultAccountConstructor(t *testing.T) (*Constructor, *mocks.Helper, *mocks.Handler) {
	helper := new(mocks.Helper)
	handler := new(mocks.Handler)
	return &Constructor{
		network: &types.NetworkIdentifier{
			Blockchain: "Ethereum",
			Network:    "Mainnet",
		},
		accountingModel: configuration.AccountModel,
		currency: &types.Currency{
			Symbol:   "ETH",
			Decimals: 18,
		},
		minimumBalance:        big.NewInt(2),
		maximumFee:            big.NewInt(100),
		curveType:             types.Secp256k1,
		newAccountProbability: 0.5,
		maxAddresses:          100,
		scenario:              configuration.EthereumTransfer,
		parser:                defaultParser(t),
		helper:                helper,
		handler:               handler,
	}, helper, handler
}

func bitcoinScenarios() ([]*types.Operation, *types.Operation) {
	changelessTransfer := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "{{ SENDER }}",
			},
			Amount: &types.Amount{
				Value: "{{ SENDER_VALUE }}",
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "{{ RECIPIENT }}",
			},
			Amount: &types.Amount{
				Value: "{{ RECIPIENT_VALUE }}",
			},
		},
	}

	change := &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 2,
		},
		Type: "Vout",
		Account: &types.AccountIdentifier{
			Address: "{{ CHANGE_ADDRESS }}",
		},
		Amount: &types.Amount{
			Value: "{{ CHANGE_VALUE }}",
		},
	}

	return changelessTransfer, change
}

func defaultUtxoConstructor(t *testing.T) (*Constructor, *mocks.Helper) {
	helper := new(mocks.Helper)
	changelessTransfer, change := bitcoinScenarios()
	return &Constructor{
		network: &types.NetworkIdentifier{
			Blockchain: "Bitcoin",
			Network:    "Mainnet",
		},
		accountingModel: configuration.UtxoModel,
		currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
		minimumBalance:        big.NewInt(600),
		maximumFee:            big.NewInt(500),
		curveType:             types.Secp256k1,
		newAccountProbability: 0.5,
		maxAddresses:          100,
		scenario:              changelessTransfer,
		changeScenario:        change,
		parser:                defaultParser(t),
		helper:                helper,
	}, helper
}

var (
	sender    = "sender"
	recipient = "recipient"
)

func TestNewAddress(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, mockHandler := defaultAccountConstructor(t)

	mockHelper.On(
		"Derive",
		ctx,
		constructor.network,
		mock.Anything,
		mock.Anything,
	).Return(
		"addr 1",
		nil,
		nil,
	)
	mockHelper.On("StoreKey", ctx, "addr 1", mock.Anything).Return(nil)
	mockHandler.On("AddressCreated", ctx, "addr 1").Return(nil)

	addr, err := constructor.newAddress(ctx)
	assert.Equal(t, "addr 1", addr)
	assert.NoError(t, err)
}

func TestCreateTransaction(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, _ := defaultAccountConstructor(t)

	senderValue := big.NewInt(100)
	recipientValue := big.NewInt(90)

	scenarioContext, scenarioOps, err := constructor.createScenarioContext(
		sender,
		senderValue,
		recipient,
		recipientValue,
		"",
		nil,
		nil,
	)
	assert.NotNil(t, scenarioContext)
	assert.NotNil(t, scenarioOps)
	assert.NoError(t, err)

	intent, err := scenario.PopulateScenario(ctx, scenarioContext, scenarioOps)
	assert.NoError(t, err)
	assert.NotNil(t, intent)

	metadataRequest := map[string]interface{}{
		"meta": "data",
	}

	metadataResponse := map[string]interface{}{
		"interesting": "stuff",
	}

	unsignedTransaction := "unsigned transaction"
	payloads := []*types.SigningPayload{
		{
			Address:       sender,
			Bytes:         []byte("signing payload"),
			SignatureType: types.Ecdsa,
		},
	}

	generatedOps := append(intent, &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 2,
		},
		Type: "fee",
		Account: &types.AccountIdentifier{
			Address: sender,
		},
		Amount: &types.Amount{
			Value:    constructor.maximumFee.String(),
			Currency: constructor.currency,
		},
	})

	signatures := []*types.Signature{
		{
			SigningPayload: payloads[0],
			PublicKey:      &types.PublicKey{},
			SignatureType:  types.Ecdsa,
			Bytes:          []byte("signature"),
		},
	}

	signedTransaction := "signed transaction"

	transactionIdentifier := &types.TransactionIdentifier{
		Hash: "transaction hash",
	}

	mockHelper.On(
		"Preprocess",
		ctx,
		constructor.network,
		intent,
		mock.Anything,
	).Return(
		metadataRequest,
		nil,
	)
	mockHelper.On(
		"Metadata",
		ctx,
		constructor.network,
		metadataRequest,
	).Return(
		metadataResponse,
		nil,
	)
	mockHelper.On(
		"Payloads",
		ctx,
		constructor.network,
		intent,
		metadataResponse,
	).Return(
		unsignedTransaction,
		payloads,
		nil,
	)
	mockHelper.On(
		"Parse",
		ctx,
		constructor.network,
		false,
		unsignedTransaction,
	).Return(
		generatedOps,
		[]string{},
		nil,
		nil,
	)
	mockHelper.On("Sign", ctx, payloads).Return(signatures, nil)
	mockHelper.On(
		"Combine",
		ctx,
		constructor.network,
		unsignedTransaction,
		signatures,
	).Return(
		signedTransaction,
		nil,
	)
	mockHelper.On(
		"Parse",
		ctx,
		constructor.network,
		true,
		signedTransaction,
	).Return(
		generatedOps,
		[]string{sender},
		nil,
		nil,
	)
	mockHelper.On(
		"Hash",
		ctx,
		constructor.network,
		signedTransaction,
	).Return(
		transactionIdentifier,
		nil,
	)

	resultIdentifier, networkTransaction, err := constructor.createTransaction(ctx, intent)
	assert.NoError(t, err)
	assert.Equal(t, transactionIdentifier, resultIdentifier)
	assert.Equal(t, signedTransaction, networkTransaction)
}

func TestMinimumRequiredBalance_Account(t *testing.T) {
	constructor, _, _ := defaultAccountConstructor(t)

	// 2 * minimum_balance + maximum_fee
	assert.Equal(t, big.NewInt(104), constructor.minimumRequiredBalance(newAccountSend))

	// minimum_balance + maximum_fee
	assert.Equal(t, big.NewInt(102), constructor.minimumRequiredBalance(existingAccountSend))
}

func TestMinimumRequiredBalance_Utxo(t *testing.T) {
	constructor, _ := defaultUtxoConstructor(t)

	// 2 * minimum_balance + maximum_fee
	assert.Equal(t, big.NewInt(1700), constructor.minimumRequiredBalance(changeSend))

	// minimum_balance + maximum_fee
	assert.Equal(t, big.NewInt(1100), constructor.minimumRequiredBalance(fullSend))
}

func TestBestUnlockedSender_Account(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, _ := defaultAccountConstructor(t)

	lockedAddresses := []string{"addr 2", "addr 4"}
	mockHelper.On("LockedAddresses", ctx).Return(lockedAddresses, nil)

	balances := map[string]*big.Int{
		"addr 1": big.NewInt(10),
		"addr 2": big.NewInt(30),
		"addr 3": big.NewInt(15),
		"addr 4": big.NewInt(1000),
		"addr 5": big.NewInt(2),
	}
	addresses := []string{}
	for k := range balances {
		mockHelper.On(
			"AccountBalance",
			ctx,
			&types.AccountIdentifier{Address: k},
			constructor.currency,
		).Return(
			balances[k],
			nil,
		)
		addresses = append(addresses, k)
	}

	bestAddress, bestBalance, bestCoin, err := constructor.bestUnlockedSender(ctx, addresses)
	assert.NoError(t, err)
	assert.Equal(t, "addr 3", bestAddress)
	assert.Equal(t, big.NewInt(15), bestBalance)
	assert.Nil(t, bestCoin)
}

func TestBestUnlockedSender_Utxo(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper := defaultUtxoConstructor(t)

	lockedAddresses := []string{"addr 2", "addr 4"}
	mockHelper.On("LockedAddresses", ctx).Return(lockedAddresses, nil)

	type intAndCoin struct {
		amount *big.Int
		coin   *types.CoinIdentifier
	}

	balances := map[string]*intAndCoin{
		"addr 1": {
			amount: big.NewInt(10),
			coin:   &types.CoinIdentifier{Identifier: "coin 1"},
		},
		"addr 2": {
			amount: big.NewInt(30),
			coin:   &types.CoinIdentifier{Identifier: "coin 2"},
		},
		"addr 3": {
			amount: big.NewInt(15),
			coin:   &types.CoinIdentifier{Identifier: "coin 3"},
		},
		"addr 4": {
			amount: big.NewInt(1000),
			coin:   &types.CoinIdentifier{Identifier: "coin 4"},
		},
		"addr 5": {
			amount: big.NewInt(2),
			coin:   &types.CoinIdentifier{Identifier: "coin 5"},
		},
	}
	addresses := []string{}
	for k := range balances {
		mockHelper.On(
			"CoinBalance",
			ctx,
			&types.AccountIdentifier{Address: k},
			constructor.currency,
		).Return(
			balances[k].amount,
			balances[k].coin,
			nil,
		)
		addresses = append(addresses, k)
	}

	bestAddress, bestBalance, bestCoin, err := constructor.bestUnlockedSender(ctx, addresses)
	assert.NoError(t, err)
	assert.Equal(t, "addr 3", bestAddress)
	assert.Equal(t, big.NewInt(15), bestBalance)
	assert.Equal(t, "coin 3", bestCoin.Identifier)
}

func TestFindSender_Load(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, mockHandler := defaultAccountConstructor(t)

	lockedAddresses := []string{}
	mockHelper.On("LockedAddresses", ctx).Return(lockedAddresses, nil)

	mockHelper.On("AllAddresses", ctx).Return([]string{}, nil).Once()

	mockHelper.On(
		"Derive",
		ctx,
		constructor.network,
		mock.Anything,
		mock.Anything,
	).Return(
		"addr 1",
		nil,
		nil,
	)
	mockHelper.On("StoreKey", ctx, "addr 1", mock.Anything).Return(nil)
	mockHandler.On("AddressCreated", ctx, "addr 1").Return(nil)

	// Deposit insufficient funds
	mockHelper.On(
		"AccountBalance",
		ctx,
		&types.AccountIdentifier{Address: "addr 1"},
		constructor.currency,
	).Return(
		big.NewInt(100),
		nil,
	).After(5 * time.Second).Once()

	// Deposit sufficient funds
	mockHelper.On(
		"AccountBalance",
		ctx,
		&types.AccountIdentifier{Address: "addr 1"},
		constructor.currency,
	).Return(
		big.NewInt(10000),
		nil,
	).After(15 * time.Second).Twice()
	mockHelper.On("AllAddresses", ctx).Return([]string{"addr 1"}, nil)

	sender, balance, coin, err := constructor.findSender(ctx)
	mockHelper.AssertExpectations(t)
	assert.NoError(t, err)
	assert.Equal(t, "addr 1", sender)
	assert.Equal(t, big.NewInt(10000), balance)
	assert.Nil(t, coin)
}

func TestFindSender_Available(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, _ := defaultAccountConstructor(t)

	lockedAddresses := []string{"addr 2", "addr 4"}
	mockHelper.On("LockedAddresses", ctx).Return(lockedAddresses, nil)

	balances := map[string]*big.Int{
		"addr 1": big.NewInt(10),
		"addr 2": big.NewInt(30),
		"addr 3": big.NewInt(15),
		"addr 4": big.NewInt(1000),
		"addr 5": big.NewInt(2),
	}
	addresses := []string{}
	for k := range balances {
		mockHelper.On(
			"AccountBalance",
			ctx,
			&types.AccountIdentifier{Address: k},
			constructor.currency,
		).Return(
			balances[k],
			nil,
		)
		addresses = append(addresses, k)
	}

	mockHelper.On("AllAddresses", ctx).Return(addresses, nil)

	sender, balance, coin, err := constructor.findSender(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "addr 3", sender)
	assert.Equal(t, big.NewInt(15), balance)
	assert.Nil(t, coin)
}

func TestFindSender_Broadcasting(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, _ := defaultAccountConstructor(t)

	// This will trigger a loop while waiting for broadcasts
	mockHelper.On(
		"LockedAddresses",
		ctx,
	).Return(
		[]string{"addr 1", "addr 2", "addr 3", "addr 4", "addr 5"},
		nil,
	).Once()

	balances := map[string]*big.Int{
		"addr 1": big.NewInt(10),
		"addr 2": big.NewInt(30),
		"addr 3": big.NewInt(15),
		"addr 4": big.NewInt(1000),
		"addr 5": big.NewInt(2),
	}
	addresses := []string{}
	for k := range balances {
		addresses = append(addresses, k)
	}

	mockHelper.On(
		"AccountBalance",
		ctx,
		&types.AccountIdentifier{Address: "addr 3"},
		constructor.currency,
	).Return(
		balances["addr 3"],
		nil,
	)

	mockHelper.On("AllAddresses", ctx).Return(addresses, nil)
	mockHelper.On("AllBroadcasts", ctx).Return([]*storage.Broadcast{{}}, nil)

	// After first fail, addr 3 will be available
	mockHelper.On(
		"LockedAddresses",
		ctx,
	).Return(
		[]string{"addr 1", "addr 2", "addr 4", "addr 5"},
		nil,
	).After(
		8 * time.Second,
	)

	sender, balance, coin, err := constructor.findSender(ctx)
	mockHelper.AssertExpectations(t)
	assert.NoError(t, err)
	assert.Equal(t, "addr 3", sender)
	assert.Equal(t, big.NewInt(15), balance)
	assert.Nil(t, coin)
}

func TestFindRecipients_Account(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, _ := defaultAccountConstructor(t)
	constructor.minimumBalance = big.NewInt(15)

	balances := map[string]*big.Int{
		"addr 1": big.NewInt(10),
		"addr 2": big.NewInt(30),
		"addr 3": big.NewInt(15),
		"addr 4": big.NewInt(1000),
		"addr 5": big.NewInt(2),
	}
	addresses := []string{}
	for k := range balances {
		mockHelper.On(
			"AccountBalance",
			ctx,
			&types.AccountIdentifier{Address: k},
			constructor.currency,
		).Return(
			balances[k],
			nil,
		)
		addresses = append(addresses, k)
	}

	mockHelper.On("AllAddresses", ctx).Return(addresses, nil)

	minimumRecipients, belowMinimumRecipients, err := constructor.findRecipients(ctx, "addr 4")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"addr 2", "addr 3"}, minimumRecipients)
	assert.ElementsMatch(t, []string{"addr 1", "addr 5"}, belowMinimumRecipients)
}

type intAndCoin struct {
	amount *big.Int
	coin   *types.CoinIdentifier
}

func TestFindRecipients_Utxo(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper := defaultUtxoConstructor(t)

	balances := map[string]*intAndCoin{
		"addr 1": {
			amount: big.NewInt(10),
			coin:   &types.CoinIdentifier{Identifier: "coin 1"},
		},
		"addr 2": {
			amount: big.NewInt(30),
			coin:   &types.CoinIdentifier{Identifier: "coin 2"},
		},
		"addr 3": {
			amount: big.NewInt(15),
			coin:   &types.CoinIdentifier{Identifier: "coin 3"},
		},
		"addr 4": {
			amount: big.NewInt(1000),
			coin:   &types.CoinIdentifier{Identifier: "coin 4"},
		},
		"addr 5": {
			amount: big.NewInt(2),
			coin:   &types.CoinIdentifier{Identifier: "coin 5"},
		},
	}
	addresses := []string{}
	for k := range balances {
		mockHelper.On(
			"CoinBalance",
			ctx,
			&types.AccountIdentifier{Address: k},
			constructor.currency,
		).Return(
			balances[k].amount,
			balances[k].coin,
			nil,
		)
		addresses = append(addresses, k)
	}

	mockHelper.On("AllAddresses", ctx).Return(addresses, nil)

	minimumRecipients, belowMinimumRecipients, err := constructor.findRecipients(ctx, "addr 4")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, minimumRecipients)
	assert.ElementsMatch(
		t,
		[]string{"addr 1", "addr 2", "addr 3", "addr 5"},
		belowMinimumRecipients,
	)
}

func TestCreateScenarioContext_Account(t *testing.T) {
	constructor, _, _ := defaultAccountConstructor(t)

	senderValue := big.NewInt(5000)
	recipientValue := big.NewInt(2000)

	scenarioContext, scenarioOps, err := constructor.createScenarioContext(
		sender,
		senderValue,
		recipient,
		recipientValue,
		"",
		nil,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, &scenario.Context{
		Sender:         sender,
		SenderValue:    senderValue,
		Recipient:      recipient,
		RecipientValue: recipientValue,
		Currency:       constructor.currency,
	}, scenarioContext)
	assert.Equal(t, constructor.scenario, scenarioOps)

	// ensure copied correctly
	constructor.scenario[0].Type = "blah"
	assert.NotEqual(t, constructor.scenario[0], scenarioOps[0])
}

func TestCreateScenarioContext_Utxo(t *testing.T) {
	constructor, _ := defaultUtxoConstructor(t)

	senderValue := big.NewInt(5000)
	recipientValue := big.NewInt(2000)
	changeAddress := "change"
	changeValue := big.NewInt(1900)
	coinIdentifier := &types.CoinIdentifier{Identifier: "coin 2"}

	scenarioContext, scenarioOps, err := constructor.createScenarioContext(
		sender,
		senderValue,
		recipient,
		recipientValue,
		changeAddress,
		changeValue,
		coinIdentifier,
	)
	assert.NoError(t, err)
	assert.Equal(t, &scenario.Context{
		Sender:         sender,
		SenderValue:    senderValue,
		Recipient:      recipient,
		RecipientValue: recipientValue,
		ChangeAddress:  changeAddress,
		ChangeValue:    changeValue,
		CoinIdentifier: coinIdentifier,
		Currency:       constructor.currency,
	}, scenarioContext)
	assert.Equal(t, append(constructor.scenario, constructor.changeScenario), scenarioOps)

	// ensure copied correctly
	constructor.scenario[0].Type = "blah"
	assert.NotEqual(t, constructor.scenario[0], scenarioOps[0])

	constructor.changeScenario.Type = "blah 2"
	assert.NotEqual(t, constructor.changeScenario, scenarioOps[2])
}

func TestCanGetNewAddress(t *testing.T) {
	ctx := context.Background()

	constructor, _, mockHandler := defaultAccountConstructor(t)

	var tests = map[string]struct {
		addresses             int
		randomAmount          *big.Int
		newAccountProbability float64
		maxAddresses          int
		recipients            []string

		addr    string
		created bool
	}{
		"no addresses": {
			addresses:             0,
			randomAmount:          big.NewInt(0),
			newAccountProbability: 0.0,
			maxAddresses:          0,
			recipients:            []string{},

			addr:    "addr 1",
			created: true,
		},
		"less than max addresses - bad flip": {
			addresses:             100,
			randomAmount:          big.NewInt(50),
			newAccountProbability: 0.25,
			maxAddresses:          200,
			recipients:            []string{"addr 6", "addr 9"},

			addr:    "addr 6",
			created: false,
		},
		"less than max addresses - good flip": {
			addresses:             100,
			randomAmount:          big.NewInt(10),
			newAccountProbability: 0.25,
			maxAddresses:          200,
			recipients:            []string{"addr 6", "addr 8"},

			addr:    "addr 1",
			created: true,
		},
		"max addresses - good flip": {
			addresses:             100,
			randomAmount:          big.NewInt(10),
			newAccountProbability: 0.25,
			maxAddresses:          100,
			recipients:            []string{"addr 6", "addr 7"},

			addr:    "addr 6",
			created: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mockHelper := new(mocks.Helper)
			constructor.helper = mockHelper

			if test.created {
				mockHelper.On(
					"Derive",
					ctx,
					constructor.network,
					mock.Anything,
					mock.Anything,
				).Return(
					"addr 1",
					nil,
					nil,
				)
				mockHelper.On("StoreKey", ctx, "addr 1", mock.Anything).Return(nil)
				mockHandler.On("AddressCreated", ctx, "addr 1").Return(nil)
			}

			addresses := []string{}
			for i := 0; i < test.addresses; i++ {
				addresses = append(addresses, fmt.Sprintf("old addr %d", i))
			}

			mockHelper.On("AllAddresses", ctx).Return(addresses, nil).Once()
			mockHelper.On(
				"RandomAmount",
				big.NewInt(0),
				big.NewInt(100),
			).Return(
				test.randomAmount,
			).Once()
			constructor.newAccountProbability = test.newAccountProbability
			constructor.maxAddresses = test.maxAddresses

			addr, created, err := constructor.canGetNewAddress(ctx, test.recipients)
			assert.NoError(t, err)
			assert.Equal(t, test.addr, addr)
			assert.Equal(t, test.created, created)
		})
	}
}

func TestGenerateScenario_Account(t *testing.T) {
	ctx := context.Background()

	constructor, _, _ := defaultAccountConstructor(t)

	newAddress := "new addr 1"

	tests := map[string]struct {
		minimumBalance        *big.Int
		maximumFee            *big.Int
		maxAddresses          int
		newAccountProbability float64
		randomAccountNumber   *big.Int
		expectNew             bool

		balances map[string]*big.Int

		senderBalance *big.Int

		sendRandomLowAmount  *big.Int
		sendAmount           *big.Int
		sendRandomHighAmount *big.Int

		scenarioCtx *scenario.Context
		scenarioOps []*types.Operation
		err         error
	}{
		"create new address - good flip": {
			minimumBalance:        big.NewInt(100),
			maximumFee:            big.NewInt(10),
			maxAddresses:          100,
			newAccountProbability: 0.5,
			randomAccountNumber:   big.NewInt(1),
			expectNew:             true,

			balances: map[string]*big.Int{
				"addr 1": big.NewInt(10),
				"addr 2": big.NewInt(30),
				"addr 3": big.NewInt(15),
				"addr 4": big.NewInt(1000),
				"addr 5": big.NewInt(2),
			},

			senderBalance: big.NewInt(1000),

			sendRandomLowAmount:  big.NewInt(100),
			sendAmount:           big.NewInt(100),
			sendRandomHighAmount: big.NewInt(890),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(100),
				Recipient:      newAddress,
				RecipientValue: big.NewInt(100),
				Currency:       constructor.currency,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"create new address - bad flip": {
			minimumBalance:        big.NewInt(100),
			maximumFee:            big.NewInt(10),
			maxAddresses:          100,
			newAccountProbability: 0.5,
			randomAccountNumber:   big.NewInt(90),
			expectNew:             false,

			balances: map[string]*big.Int{
				"addr 1": big.NewInt(10),
				"addr 2": big.NewInt(30),
				"addr 3": big.NewInt(15),
				"addr 4": big.NewInt(1000),
				"addr 5": big.NewInt(2),
			},

			senderBalance: big.NewInt(1000),

			sendRandomLowAmount:  big.NewInt(0),
			sendAmount:           big.NewInt(100),
			sendRandomHighAmount: big.NewInt(890),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(100),
				Recipient:      "addr 4",
				RecipientValue: big.NewInt(100),
				Currency:       constructor.currency,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"can't afford new address": {
			minimumBalance:        big.NewInt(500),
			maximumFee:            big.NewInt(100),
			maxAddresses:          100,
			newAccountProbability: 0.5,
			randomAccountNumber:   big.NewInt(1),
			expectNew:             false,

			balances: map[string]*big.Int{
				"addr 1": big.NewInt(10),
				"addr 2": big.NewInt(30),
				"addr 3": big.NewInt(15),
				"addr 4": big.NewInt(1000),
				"addr 5": big.NewInt(2),
			},

			senderBalance: big.NewInt(1000),

			sendRandomLowAmount:  big.NewInt(0),
			sendAmount:           big.NewInt(100),
			sendRandomHighAmount: big.NewInt(400),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(100),
				Recipient:      "addr 4",
				RecipientValue: big.NewInt(100),
				Currency:       constructor.currency,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"can't afford new address but no addresses above minimum": {
			minimumBalance:        big.NewInt(500),
			maximumFee:            big.NewInt(100),
			maxAddresses:          100,
			newAccountProbability: 0.5,
			randomAccountNumber:   big.NewInt(1),
			expectNew:             false,

			balances: map[string]*big.Int{},

			senderBalance: big.NewInt(1000),

			sendAmount: big.NewInt(100),

			scenarioCtx: nil,
			scenarioOps: nil,
			err:         ErrInsufficientFunds,
		},
		"zero funds": {
			minimumBalance:        big.NewInt(0),
			maximumFee:            big.NewInt(0),
			maxAddresses:          100,
			newAccountProbability: 0.5,
			randomAccountNumber:   big.NewInt(1),

			balances: map[string]*big.Int{
				"addr 1": big.NewInt(10),
				"addr 2": big.NewInt(30),
				"addr 3": big.NewInt(15),
				"addr 4": big.NewInt(1000),
				"addr 5": big.NewInt(2),
			},

			senderBalance: big.NewInt(0),

			sendAmount: big.NewInt(0),

			scenarioCtx: nil,
			scenarioOps: nil,
			err:         ErrInsufficientFunds,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// reset at start of each loop
			mockHelper := new(mocks.Helper)
			constructor.helper = mockHelper

			mockHandler := new(mocks.Handler)
			constructor.handler = mockHandler

			mockHelper.On(
				"RandomAmount",
				big.NewInt(0),
				big.NewInt(100),
			).Return(
				test.randomAccountNumber,
			).Once()
			if test.expectNew {
				mockHelper.On(
					"Derive",
					ctx,
					constructor.network,
					mock.Anything,
					mock.Anything,
				).Return(
					newAddress,
					nil,
					nil,
				).Once()
				mockHelper.On("StoreKey", ctx, newAddress, mock.Anything).Return(nil)
				mockHandler.On("AddressCreated", ctx, newAddress).Return(nil).Once()
			}

			constructor.minimumBalance = test.minimumBalance
			constructor.maxAddresses = test.maxAddresses
			constructor.maximumFee = test.maximumFee
			constructor.newAccountProbability = test.newAccountProbability

			if test.sendRandomLowAmount != nil && test.sendRandomHighAmount != nil {
				mockHelper.On(
					"RandomAmount",
					test.sendRandomLowAmount,
					test.sendRandomHighAmount,
				).Return(
					test.sendAmount,
				)
			}

			// Lock in addresses order so don't introduce flaky test (instead of
			// iterating over balance map)
			addresses := []string{}
			for i := 0; i < len(test.balances); i++ {
				addresses = append(addresses, fmt.Sprintf("addr %d", i+1))
			}

			mockHelper.On("AllAddresses", ctx).Return(addresses, nil)

			for _, k := range addresses {
				mockHelper.On(
					"AccountBalance",
					ctx,
					&types.AccountIdentifier{Address: k},
					constructor.currency,
				).Return(
					test.balances[k],
					nil,
				)
			}

			scenarioCtx, scenarioOps, err := constructor.generateScenario(
				ctx,
				sender,
				test.senderBalance,
				nil,
			)
			if test.err != nil {
				assert.Equal(t, test.err, err)
				assert.Nil(t, scenarioCtx)
				assert.Nil(t, scenarioOps)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.scenarioCtx, scenarioCtx)
				assert.Equal(t, test.scenarioOps, scenarioOps)
			}
		})
	}
}

func TestGenerateScenario_Utxo(t *testing.T) {
	ctx := context.Background()

	constructor, _ := defaultUtxoConstructor(t)

	senderCoin := &types.CoinIdentifier{Identifier: "sender coin"}
	newAddress := "new addr 1"
	changeAddress := "change addr 1"

	tests := map[string]struct {
		minimumBalance        *big.Int
		maximumFee            *big.Int
		maxAddresses          int
		newAccountProbability float64

		recipientAccountNumber *big.Int
		expectNewRecipient     bool

		changeAccountNumber *big.Int
		expectNewChange     bool

		changeDifferential       *big.Int
		changeDifferentialAmount *big.Int

		addresses int

		senderBalance *big.Int

		scenarioCtx *scenario.Context
		scenarioOps []*types.Operation
		err         error
	}{
		"create new recipient, create new change - good flips": {
			minimumBalance:         big.NewInt(400),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(1),
			expectNewRecipient:     true,
			changeAccountNumber:    big.NewInt(1),
			expectNewChange:        true,

			changeDifferential:       big.NewInt(300),
			changeDifferentialAmount: big.NewInt(151),

			addresses: 10,

			senderBalance: big.NewInt(1500),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1500),
				Recipient:      newAddress,
				RecipientValue: big.NewInt(551),
				ChangeAddress:  changeAddress,
				ChangeValue:    big.NewInt(549),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: append(constructor.scenario, constructor.changeScenario),
			err:         nil,
		},
		"create new recipient, create new change - bad flips": {
			minimumBalance:         big.NewInt(400),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(95),
			expectNewRecipient:     true,
			changeAccountNumber:    big.NewInt(95),
			expectNewChange:        true,

			changeDifferential:       big.NewInt(300),
			changeDifferentialAmount: big.NewInt(151),

			addresses: 0,

			senderBalance: big.NewInt(1500),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1500),
				Recipient:      newAddress,
				RecipientValue: big.NewInt(551),
				ChangeAddress:  changeAddress,
				ChangeValue:    big.NewInt(549),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: append(constructor.scenario, constructor.changeScenario),
			err:         nil,
		},
		"existing recipient, create new change": {
			minimumBalance:         big.NewInt(400),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(95),
			expectNewRecipient:     false,
			changeAccountNumber:    big.NewInt(1),
			expectNewChange:        true,

			changeDifferential:       big.NewInt(300),
			changeDifferentialAmount: big.NewInt(151),

			addresses: 10,

			senderBalance: big.NewInt(1500),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1500),
				Recipient:      "addr 1",
				RecipientValue: big.NewInt(551),
				ChangeAddress:  changeAddress,
				ChangeValue:    big.NewInt(549),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: append(constructor.scenario, constructor.changeScenario),
			err:         nil,
		},
		"existing recipient, existing change": {
			minimumBalance:         big.NewInt(400),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(95),
			expectNewRecipient:     false,
			changeAccountNumber:    big.NewInt(90),
			expectNewChange:        false,

			changeDifferential:       big.NewInt(300),
			changeDifferentialAmount: big.NewInt(151),

			addresses: 10,

			senderBalance: big.NewInt(1500),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1500),
				Recipient:      "addr 1",
				RecipientValue: big.NewInt(551),
				ChangeAddress:  "addr 2",
				ChangeValue:    big.NewInt(549),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: append(constructor.scenario, constructor.changeScenario),
			err:         nil,
		},
		"create new address, no change - good flip": {
			minimumBalance:         big.NewInt(500),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(1),
			expectNewRecipient:     true,

			addresses: 10,

			senderBalance: big.NewInt(1000),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1000),
				Recipient:      newAddress,
				RecipientValue: big.NewInt(600),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"don't create new address, no change - good flip": {
			minimumBalance:         big.NewInt(500),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(1),
			expectNewRecipient:     false,

			addresses: 200,

			senderBalance: big.NewInt(1000),

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1000),
				Recipient:      "addr 1",
				RecipientValue: big.NewInt(600),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"create new address, no change - bad flip": {
			minimumBalance:         big.NewInt(500),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(90),
			expectNewRecipient:     false,

			senderBalance: big.NewInt(1000),

			addresses: 10,

			scenarioCtx: &scenario.Context{
				Sender:         sender,
				SenderValue:    big.NewInt(1000),
				Recipient:      "addr 1",
				RecipientValue: big.NewInt(600),
				Currency:       constructor.currency,
				CoinIdentifier: senderCoin,
			},
			scenarioOps: constructor.scenario,
			err:         nil,
		},
		"insufficient funds": {
			minimumBalance:         big.NewInt(500),
			maximumFee:             big.NewInt(400),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(90),
			expectNewRecipient:     false,

			senderBalance: big.NewInt(500),

			addresses: 10,

			scenarioCtx: nil,
			scenarioOps: nil,
			err:         ErrInsufficientFunds,
		},
		"zero funds": {
			minimumBalance:         big.NewInt(0),
			maximumFee:             big.NewInt(0),
			maxAddresses:           100,
			newAccountProbability:  0.5,
			recipientAccountNumber: big.NewInt(1),

			senderBalance: big.NewInt(0),

			addresses: 10,

			scenarioCtx: nil,
			scenarioOps: nil,
			err:         ErrInsufficientFunds,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// reset at start of each loop
			mockHelper := new(mocks.Helper)
			constructor.helper = mockHelper

			mockHandler := new(mocks.Handler)
			constructor.handler = mockHandler

			mockHelper.On(
				"RandomAmount",
				big.NewInt(0),
				big.NewInt(100),
			).Return(
				test.recipientAccountNumber,
			).Once()
			if test.expectNewRecipient {
				mockHelper.On(
					"Derive",
					ctx,
					constructor.network,
					mock.Anything,
					mock.Anything,
				).Return(
					newAddress,
					nil,
					nil,
				).Once()
				mockHelper.On("StoreKey", ctx, newAddress, mock.Anything).Return(nil).Once()
				mockHandler.On("AddressCreated", ctx, newAddress).Return(nil).Once()
			}

			mockHelper.On(
				"RandomAmount",
				big.NewInt(0),
				big.NewInt(100),
			).Return(
				test.changeAccountNumber,
			).Once()
			if test.expectNewChange {
				mockHelper.On(
					"Derive",
					ctx,
					constructor.network,
					mock.Anything,
					mock.Anything,
				).Return(
					changeAddress,
					nil,
					nil,
				).Once()
				mockHelper.On("StoreKey", ctx, changeAddress, mock.Anything).Return(nil).Once()
				mockHandler.On("AddressCreated", ctx, changeAddress).Return(nil).Once()
			}

			if test.changeDifferential != nil {
				mockHelper.On(
					"RandomAmount",
					big.NewInt(0),
					test.changeDifferential,
				).Return(
					test.changeDifferentialAmount,
				).Once()
			}

			constructor.minimumBalance = test.minimumBalance
			constructor.maxAddresses = test.maxAddresses
			constructor.maximumFee = test.maximumFee
			constructor.newAccountProbability = test.newAccountProbability

			// Lock in addresses order so don't introduce flaky test (instead of
			// iterating over balance map)
			addresses := []string{}
			for i := 0; i < test.addresses; i++ {
				addresses = append(addresses, fmt.Sprintf("addr %d", i+1))
			}

			mockHelper.On("AllAddresses", ctx).Return(addresses, nil)

			scenarioCtx, scenarioOps, err := constructor.generateScenario(
				ctx,
				sender,
				test.senderBalance,
				senderCoin,
			)
			if test.err != nil {
				assert.Equal(t, test.err, err)
				assert.Nil(t, scenarioCtx)
				assert.Nil(t, scenarioOps)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.scenarioCtx, scenarioCtx)
				assert.Equal(t, test.scenarioOps, scenarioOps)
			}
		})
	}
}
