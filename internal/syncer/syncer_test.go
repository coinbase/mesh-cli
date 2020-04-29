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

package syncer

import (
	"context"
	"testing"

	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func simpleTransactionFactory(
	hash string,
	address string,
	value string,
	currency *types.Currency,
) *types.Transaction {
	return &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: hash,
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 0,
				},
				Type:   "Transfer",
				Status: "Success",
				Account: &types.AccountIdentifier{
					Address: address,
				},
				Amount: &types.Amount{
					Value:    value,
					Currency: currency,
				},
			},
		},
	}
}

func TestBalanceChanges(t *testing.T) {
	var tests = map[string]struct {
		block          *types.Block
		orphan         bool
		changes        []*storage.BalanceChange
		exemptAccounts []*reconciler.AccountCurrency
		err            error
	}{
		"simple block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					recipientTransaction,
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: false,
			changes: []*storage.BalanceChange{
				{
					Account:  recipient,
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "100",
				},
			},
			err: nil,
		},
		"simple block account exempt": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					recipientTransaction,
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan:  false,
			changes: []*storage.BalanceChange{},
			exemptAccounts: []*reconciler.AccountCurrency{
				{
					Account:  recipient,
					Currency: currency,
				},
			},
			err: nil,
		},
		"single account sum block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: false,
			changes: []*storage.BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "150",
				},
			},
			err: nil,
		},
		"single account sum orphan block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: true,
			changes: []*storage.BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "0",
						Index: 0,
					},
					Difference: "-250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "0",
						Index: 0,
					},
					Difference: "-150",
				},
			},
			err: nil,
		},
	}

	ctx := context.Background()
	asserter, err := asserter.NewClientWithResponses(
		networkIdentifier,
		networkStatusResponse,
		networkOptionsResponse,
	)
	assert.NotNil(t, asserter)
	assert.NoError(t, err)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := NewBaseHandler(nil, nil, test.exemptAccounts)
			changes, err := BalanceChanges(
				ctx,
				asserter,
				test.block,
				test.orphan,
				handler,
			)

			assert.ElementsMatch(t, test.changes, changes)
			assert.Equal(t, test.err, err)
		})
	}
}
