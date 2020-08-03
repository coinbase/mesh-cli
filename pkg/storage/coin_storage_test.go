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

package storage

import (
	"context"
	"math/big"
	"testing"

	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	account = &types.AccountIdentifier{
		Address: "blah",
	}

	account2 = &types.AccountIdentifier{
		Address: "blah2",
	}

	account3 = &types.AccountIdentifier{
		Address: "blah",
		SubAccount: &types.SubAccountIdentifier{
			Address: "extra account",
		},
	}

	accountCoins = []*Coin{
		{
			Identifier:  &types.CoinIdentifier{Identifier: "coin1"},
			Transaction: coinBlock.Transactions[0],
			Operation:   coinBlock.Transactions[0].Operations[0],
		},
	}

	account2Coins = []*Coin{
		{
			Identifier:  &types.CoinIdentifier{Identifier: "coin2"},
			Transaction: coinBlock.Transactions[0],
			Operation:   coinBlock.Transactions[0].Operations[1],
		},
	}

	account3Coins = []*Coin{
		{
			Identifier:  &types.CoinIdentifier{Identifier: "coin3"},
			Transaction: coinBlock3.Transactions[0],
			Operation:   coinBlock3.Transactions[0].Operations[0],
		},
		{
			Identifier:  &types.CoinIdentifier{Identifier: "coin4"},
			Transaction: coinBlock3.Transactions[1],
			Operation:   coinBlock3.Transactions[1].Operations[0],
		},
	}

	successStatus = "success"
	failureStatus = "failure"

	currency = &types.Currency{
		Symbol:   "sym",
		Decimals: 12,
	}

	currency2 = &types.Currency{
		Symbol:   "sym2",
		Decimals: 12,
	}

	coinBlock = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "10",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin1",
							},
						},
					},
					{
						Account: account2,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "15",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin2",
							},
						},
					},
					{
						Account: account2,
						Status:  failureStatus,
						Amount: &types.Amount{
							Value:    "20",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin2",
							},
						},
					},
				},
			},
		},
	}

	coinBlock2 = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "-10",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin1",
							},
						},
					},
				},
			},
		},
	}

	coinBlock3 = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "4",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin3",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "6",
							Currency: currency2,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin4",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  failureStatus,
						Amount: &types.Amount{
							Value:    "12",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin5",
							},
						},
					},
				},
			},
		},
	}
)

func TestCoinStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	a, err := asserter.NewClientWithOptions(
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
				Status:     successStatus,
				Successful: true,
			},
			{
				Status:     failureStatus,
				Successful: false,
			},
		},
		[]*types.Error{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, a)

	c := NewCoinStorage(database, a)

	t.Run("get coins of unset account", func(t *testing.T) {
		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*Coin{}, coins)

		bal, coinIdentifier, err := c.GetLargestCoin(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(0), bal)
		assert.Nil(t, coinIdentifier)
	})

	t.Run("add block", func(t *testing.T) {
		tx := c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err := c.AddingBlock(ctx, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
	})

	t.Run("add duplicate coin", func(t *testing.T) {
		tx := c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err := c.AddingBlock(ctx, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.Error(t, err)
		tx.Discard(ctx)

		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
	})

	t.Run("remove block", func(t *testing.T) {
		tx := c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err := c.RemovingBlock(ctx, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*Coin{}, coins)

		coins, err = c.GetCoins(ctx, account2)
		assert.NoError(t, err)
		assert.Equal(t, account2Coins, coins)
	})

	t.Run("spend coin", func(t *testing.T) {
		tx := c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err := c.AddingBlock(ctx, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)

		tx = c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err = c.AddingBlock(ctx, coinBlock2, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		coins, err = c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*Coin{}, coins)

		coins, err = c.GetCoins(ctx, account2)
		assert.NoError(t, err)
		assert.Equal(t, []*Coin{}, coins)
	})

	t.Run("add block with multiple outputs for 1 account", func(t *testing.T) {
		tx := c.db.NewDatabaseTransaction(ctx, true)
		commitFunc, err := c.AddingBlock(ctx, coinBlock3, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		coins, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*Coin{}, coins)

		coins, err = c.GetCoins(ctx, account3)
		assert.NoError(t, err)
		assert.ElementsMatch(t, account3Coins, coins)

		bal, coinIdentifier, err := c.GetLargestCoin(ctx, account3, currency)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(4), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin3"}, coinIdentifier)

		bal, coinIdentifier, err = c.GetLargestCoin(ctx, account3, currency2)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(6), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin4"}, coinIdentifier)
	})
}
