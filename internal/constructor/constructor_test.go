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
	mocks "github.com/coinbase/rosetta-cli/mocks/constructor"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func defaultConstructor() (*Constructor, *mocks.Helper, *mocks.Handler) {
	helper := new(mocks.Helper)
	handler := new(mocks.Handler)
	return &Constructor{
		network: &types.NetworkIdentifier{
			Blockchain: "Bitcoin",
			Network:    "Mainnet",
		},
		accountingModel: configuration.AccountModel,
		currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
		minimumBalance:        big.NewInt(0),
		maximumFee:            big.NewInt(100),
		curveType:             types.Secp256k1,
		newAccountProbability: 0.5,
		maxAddresses:          100,
		scenario:              configuration.EthereumTransfer,
		helper:                helper,
		handler:               handler,
	}, helper, handler
}

func TestNewAddress(t *testing.T) {
	ctx := context.Background()

	constructor, mockHelper, mockHandler := defaultConstructor()

	mockHelper.On("Derive", ctx, constructor.network, mock.Anything, mock.Anything).Return("addr 1", nil, nil)
	mockHelper.On("StoreKey", ctx, "addr 1", mock.Anything).Return(nil)
	mockHandler.On("AddressCreated", ctx, "addr 1").Return(nil)

	addr, err := constructor.newAddress(ctx)
	assert.Equal(t, "addr 1", addr)
	assert.NoError(t, err)
}
