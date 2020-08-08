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

package scenario

import (
	"context"
	"math/big"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	sender         = "addr1"
	senderValue    = big.NewInt(100)
	recipient      = "addr2"
	recipientValue = big.NewInt(70)
	coinIdentifier = &types.CoinIdentifier{Identifier: "utxo1"}
	changeAddress  = "addr3"
	changeValue    = big.NewInt(20)

	bitcoinCurrency = &types.Currency{
		Symbol:   "BTC",
		Decimals: 8,
	}
	ethereumCurrency = &types.Currency{
		Symbol:   "ETH",
		Decimals: 18,
	}
)

func TestPopulateScenario(t *testing.T) {
	var tests = map[string]struct {
		context  *Context
		scenario []*types.Operation

		expected []*types.Operation
	}{
		"bitcoin": {
			context: &Context{
				Sender:         sender,
				SenderValue:    senderValue,
				Recipient:      recipient,
				RecipientValue: recipientValue,
				CoinIdentifier: coinIdentifier,
				Currency:       bitcoinCurrency,
				ChangeAddress:  changeAddress,
				ChangeValue:    changeValue,
			},
			scenario: []*types.Operation{
				{
					Type: "Vin",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
					Account: &types.AccountIdentifier{
						Address: "{{ SENDER }}",
					},
					Amount: &types.Amount{
						Value: "{{ SENDER_VALUE }}",
					},
					CoinChange: &types.CoinChange{
						CoinAction: types.CoinSpent,
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "{{ COIN_IDENTIFIER }}",
						},
					},
				},
				{
					Type: "Vout",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					Account: &types.AccountIdentifier{
						Address: "{{ RECIPIENT }}",
					},
					Amount: &types.Amount{
						Value: "{{ RECIPIENT_VALUE }}",
					},
				},
				{
					Type: "Vout",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
					Account: &types.AccountIdentifier{
						Address: "{{ CHANGE_ADDRESS }}",
					},
					Amount: &types.Amount{
						Value: "{{ CHANGE_VALUE }}",
					},
				},
			},
			expected: []*types.Operation{
				{
					Type: "Vin",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
					Account: &types.AccountIdentifier{
						Address: sender,
					},
					Amount: &types.Amount{
						Value:    new(big.Int).Neg(senderValue).String(),
						Currency: bitcoinCurrency,
					},
					CoinChange: &types.CoinChange{
						CoinAction:     types.CoinSpent,
						CoinIdentifier: coinIdentifier,
					},
				},
				{
					Type: "Vout",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					Account: &types.AccountIdentifier{
						Address: recipient,
					},
					Amount: &types.Amount{
						Value:    new(big.Int).Abs(recipientValue).String(),
						Currency: bitcoinCurrency,
					},
				},
				{
					Type: "Vout",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
					Account: &types.AccountIdentifier{
						Address: changeAddress,
					},
					Amount: &types.Amount{
						Value:    new(big.Int).Abs(changeValue).String(),
						Currency: bitcoinCurrency,
					},
				},
			},
		},
		"ethereum": {
			context: &Context{
				Sender:         sender,
				SenderValue:    senderValue,
				Recipient:      recipient,
				RecipientValue: recipientValue,
				Currency:       ethereumCurrency,
			},
			scenario: []*types.Operation{
				{
					Type: "transfer",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
					Account: &types.AccountIdentifier{
						Address: "{{ SENDER }}",
					},
					Amount: &types.Amount{
						Value: "{{ SENDER_VALUE }}",
					},
				},
				{
					Type: "transfer",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 0,
						},
					},
					Account: &types.AccountIdentifier{
						Address: "{{ RECIPIENT }}",
					},
					Amount: &types.Amount{
						Value: "{{ RECIPIENT_VALUE }}",
					},
				},
			},
			expected: []*types.Operation{
				{
					Type: "transfer",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
					Account: &types.AccountIdentifier{
						Address: sender,
					},
					Amount: &types.Amount{
						Value:    new(big.Int).Neg(senderValue).String(),
						Currency: ethereumCurrency,
					},
				},
				{
					Type: "transfer",
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 0,
						},
					},
					Account: &types.AccountIdentifier{
						Address: recipient,
					},
					Amount: &types.Amount{
						Value:    new(big.Int).Abs(recipientValue).String(),
						Currency: ethereumCurrency,
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			ops, err := PopulateScenario(
				ctx,
				test.context,
				test.scenario,
			)
			assert.NoError(t, err)
			assert.ElementsMatch(t, test.expected, ops)
		})
	}
}
