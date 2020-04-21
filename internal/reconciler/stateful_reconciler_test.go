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
	reconciler := NewStateful(nil, blockStorage, nil, logger, 1, false, true)

	t.Run("No head block yet", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
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
			account1,
			currency1,
			amount1.Value,
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
			account1,
			currency1,
			amount1.Value,
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
			account1,
			currency1,
			amount1.Value,
			block0,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), ErrAccountUpdated.Error())
	})

	t.Run("Account balance matches", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Account balance matches later live block", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Balances are not equal", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount2.Value,
			block2,
		)
		assert.Equal(t, "-100", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Compare balance for non-existent account", func(t *testing.T) {
		difference, headIndex, err := reconciler.CompareBalance(
			ctx,
			account2,
			currency1,
			amount2.Value,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), storage.ErrAccountNotFound.Error())
	})
}
