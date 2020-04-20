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

package reconciler

import (
	"context"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestContainsAccountAndCurrency(t *testing.T) {
	currency1 := &types.Currency{
		Symbol:   "Blah",
		Decimals: 2,
	}
	currency2 := &types.Currency{
		Symbol:   "Blah2",
		Decimals: 2,
	}
	accts := []*storage.BalanceChange{
		{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency1,
		},
		{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
				},
			},
			Currency: currency1,
		},
		{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: &map[string]interface{}{
						"neat": "stuff",
					},
				},
			},
			Currency: currency1,
		},
	}

	t.Run("Non-existent account", func(t *testing.T) {
		assert.False(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "blah",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account", func(t *testing.T) {
		assert.True(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account with bad currency", func(t *testing.T) {
		assert.False(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency2,
		}))
	})

	t.Run("Account with subaccount", func(t *testing.T) {
		assert.True(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
				},
			},
			Currency: currency1,
		}))
	})

	t.Run("Account with subaccount and metadata", func(t *testing.T) {
		assert.True(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: &map[string]interface{}{
						"neat": "stuff",
					},
				},
			},
			Currency: currency1,
		}))
	})

	t.Run("Account with subaccount and unique metadata", func(t *testing.T) {
		assert.False(t, containsAccountAndCurrency(accts, &storage.BalanceChange{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: &map[string]interface{}{
						"neater": "stuff",
					},
				},
			},
			Currency: currency1,
		}))
	})
}

func TestExtractAmount(t *testing.T) {
	var (
		account1 = &types.AccountIdentifier{
			Address: "blah",
		}

		account2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "sub blah",
			},
		}

		currency1 = &types.Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		currency2 = &types.Currency{
			Symbol:   "curr2",
			Decimals: 7,
		}

		amount1 = &types.Amount{
			Value:    "100",
			Currency: currency1,
		}

		amount2 = &types.Amount{
			Value:    "200",
			Currency: currency2,
		}

		balances = []*types.Amount{
			amount1,
			amount2,
		}

		badCurr = &storage.BalanceChange{
			Account: account1,
			Currency: &types.Currency{
				Symbol:   "no curr",
				Decimals: 100,
			},
		}
	)

	t.Run("Non-existent currency", func(t *testing.T) {
		result, err := extractAmount(balances, badCurr)
		assert.Nil(t, result)
		assert.EqualError(t, err, fmt.Errorf("could not extract amount for %+v", badCurr).Error())
	})

	t.Run("Simple account", func(t *testing.T) {
		result, err := extractAmount(balances, &storage.BalanceChange{
			Account:  account1,
			Currency: currency1,
		})
		assert.Equal(t, amount1, result)
		assert.NoError(t, err)
	})

	t.Run("SubAccount", func(t *testing.T) {
		result, err := extractAmount(balances, &storage.BalanceChange{
			Account:  account2,
			Currency: currency2,
		})
		assert.Equal(t, amount2, result)
		assert.NoError(t, err)
	})
}

func TestCompareBalance(t *testing.T) {
	var (
		account1 = &types.AccountIdentifier{
			Address: "blah",
		}

		account2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "sub blah",
			},
		}

		currency1 = &types.Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		currency2 = &types.Currency{
			Symbol:   "curr2",
			Decimals: 7,
		}

		amount1 = &types.Amount{
			Value:    "100",
			Currency: currency1,
		}

		amount2 = &types.Amount{
			Value:    "200",
			Currency: currency2,
		}

		block0 = &types.BlockIdentifier{
			Hash:  "block0",
			Index: 0,
		}

		block1 = &types.BlockIdentifier{
			Hash:  "block1",
			Index: 1,
		}

		block2 = &types.BlockIdentifier{
			Hash:  "block2",
			Index: 2,
		}

		ctx = context.Background()
	)

	newDir, err := storage.CreateTempDir()
	assert.NoError(t, err)
	defer storage.RemoveTempDir(*newDir)

	database, err := storage.NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	blockStorage := storage.NewBlockStorage(ctx, database)
	logger := logger.NewLogger(*newDir, false, false, false)
	reconciler := New(ctx, nil, blockStorage, nil, logger, 1, false)

	t.Run("No head block yet", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(0), headIndex)
		assert.EqualError(t, err, storage.ErrHeadBlockNotFound.Error())
	})

	// Update head block
	txn := blockStorage.NewDatabaseTransaction(ctx, true)
	err = blockStorage.StoreHeadBlockIdentifier(ctx, txn, block0)
	assert.NoError(t, err)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	t.Run("Live block is ahead of head block", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(0), headIndex)
		assert.EqualError(t, err, fmt.Errorf(
			"%w live block %d > head block %d",
			ErrHeadBlockBehindLive,
			1,
			0,
		).Error())
	})

	// Update head block
	txn = blockStorage.NewDatabaseTransaction(ctx, true)
	err = blockStorage.StoreHeadBlockIdentifier(ctx, txn, &types.BlockIdentifier{
		Hash:  "hash2",
		Index: 2,
	})
	assert.NoError(t, err)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	t.Run("Live block is not in store", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), ErrBlockGone.Error())
	})

	// Add blocks to store behind head
	txn = blockStorage.NewDatabaseTransaction(ctx, true)
	err = blockStorage.StoreBlock(ctx, txn, &types.Block{
		BlockIdentifier:       block0,
		ParentBlockIdentifier: block0,
	})
	assert.NoError(t, err)

	err = blockStorage.StoreBlock(ctx, txn, &types.Block{
		BlockIdentifier:       block1,
		ParentBlockIdentifier: block0,
	})
	assert.NoError(t, err)

	err = blockStorage.StoreBlock(ctx, txn, &types.Block{
		BlockIdentifier:       block2,
		ParentBlockIdentifier: block1,
	})
	assert.NoError(t, err)

	_, err = blockStorage.UpdateBalance(ctx, txn, account1, amount1, block1)
	assert.NoError(t, err)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	t.Run("Account updated after live block", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block0,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), ErrAccountUpdated.Error())
	})

	t.Run("Account balance matches", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Account balance matches later live block", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount1,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Balances are not equal", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account1,
				Currency: currency1,
			},
			amount2,
			block2,
		)
		assert.Equal(t, "-100", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Compare balance for non-existent account", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			&storage.BalanceChange{
				Account:  account2,
				Currency: currency1,
			},
			amount2,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), storage.ErrAccountNotFound.Error())
	})
}
