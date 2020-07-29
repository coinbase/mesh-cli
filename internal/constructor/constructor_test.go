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
	"math/big"
	"testing"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/internal/scenario"
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

func defaultUtxoConstructor(t *testing.T) (*Constructor, *mocks.Helper, *mocks.Handler) {
	helper := new(mocks.Helper)
	handler := new(mocks.Handler)
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
		handler:               handler,
	}, helper, handler
}

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

	sender := "sender"
	senderValue := big.NewInt(100)
	recipient := "recipient"
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
	constructor, _, _ := defaultUtxoConstructor(t)

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
		mockHelper.On("AccountBalance", ctx, &types.AccountIdentifier{Address: k}, constructor.currency).Return(balances[k], nil)
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

	constructor, mockHelper, _ := defaultUtxoConstructor(t)

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
		mockHelper.On("CoinBalance", ctx, &types.AccountIdentifier{Address: k}, constructor.currency).Return(balances[k].amount, balances[k].coin, nil)
		addresses = append(addresses, k)
	}

	bestAddress, bestBalance, bestCoin, err := constructor.bestUnlockedSender(ctx, addresses)
	assert.NoError(t, err)
	assert.Equal(t, "addr 3", bestAddress)
	assert.Equal(t, big.NewInt(15), bestBalance)
	assert.Equal(t, "coin 3", bestCoin.Identifier)
}
