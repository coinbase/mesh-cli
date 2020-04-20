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
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

var (
	currency = &types.Currency{
		Symbol:   "Blah",
		Decimals: 2,
	}

	recipient = &types.AccountIdentifier{
		Address: "acct1",
	}

	recipientAmount = &types.Amount{
		Value:    "100",
		Currency: currency,
	}

	recipientOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 0,
		},
		Type:    "Transfer",
		Status:  "Success",
		Account: recipient,
		Amount:  recipientAmount,
	}

	recipientFailureOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 1,
		},
		Type:    "Transfer",
		Status:  "Failure",
		Account: recipient,
		Amount:  recipientAmount,
	}

	recipientTransaction = &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx1",
		},
		Operations: []*types.Operation{
			recipientOperation,
			recipientFailureOperation,
		},
	}

	sender = &types.AccountIdentifier{
		Address: "acct2",
	}

	senderAmount = &types.Amount{
		Value:    "-100",
		Currency: currency,
	}

	senderOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 0,
		},
		Type:    "Transfer",
		Status:  "Success",
		Account: sender,
		Amount:  senderAmount,
	}

	senderTransaction = &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx2",
		},
		Operations: []*types.Operation{
			senderOperation,
		},
	}

	orphanGenesis = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "1",
			Index: 1,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "0a",
			Index: 0,
		},
		Transactions: []*types.Transaction{},
	}

	blockSequence = []*types.Block{
		{ // genesis
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
		},
		{
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
		},
		{ // reorg
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "2",
				Index: 2,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "1a",
				Index: 1,
			},
		},
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "1a",
				Index: 1,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
		},
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "3",
				Index: 3,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "2",
				Index: 2,
			},
			Transactions: []*types.Transaction{
				senderTransaction,
			},
		},
		{ // invalid block
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "5",
				Index: 5,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "4",
				Index: 4,
			},
		},
	}

	operationStatuses = []*types.OperationStatus{
		{
			Status:     "Success",
			Successful: true,
		},
		{
			Status:     "Failure",
			Successful: false,
		},
	}

	networkStatusResponse = &types.NetworkStatusResponse{
		GenesisBlockIdentifier: &types.BlockIdentifier{
			Index: 0,
			Hash:  "block 0",
		},
		CurrentBlockIdentifier: &types.BlockIdentifier{
			Index: 10000,
			Hash:  "block 1000",
		},
		CurrentBlockTimestamp: 10000,
		Peers: []*types.Peer{
			{
				PeerID: "peer 1",
			},
		},
	}

	networkOptionsResponse = &types.NetworkOptionsResponse{
		Version: &types.Version{
			RosettaVersion: "1.3.1",
			NodeVersion:    "1.0",
		},
		Allow: &types.Allow{
			OperationStatuses: operationStatuses,
			OperationTypes: []string{
				"Transfer",
			},
		},
	}
)

// assertNextSyncableRange is a helper function used to test
// the nextSyncableRange function during block processing.
func assertNextSyncableRange(
	ctx context.Context,
	t *testing.T,
	syncer *StatefulSyncer,
	currIndex int64,
) {
	genesisIndex, startIndex, endIndex, err := syncer.nextSyncableRange(
		ctx,
		networkStatusResponse,
	)

	assert.Equal(t, int64(0), genesisIndex)
	assert.Equal(t, currIndex, startIndex)
	assert.Equal(t, currIndex+maxSync, endIndex)
	assert.NoError(t, err)
}

func TestProcessBlock(t *testing.T) {
	ctx := context.Background()

	newDir, err := storage.CreateTempDir()
	assert.NoError(t, err)
	defer storage.RemoveTempDir(*newDir)

	database, err := storage.NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	blockStorage := storage.NewBlockStorage(ctx, database)
	asserter, err := asserter.NewWithResponses(
		ctx,
		networkStatusResponse,
		networkOptionsResponse,
	)
	assert.NotNil(t, asserter)
	assert.NoError(t, err)

	fetcher := &fetcher.Fetcher{
		Asserter: asserter,
	}
	syncer := NewStateful(nil, blockStorage, fetcher, nil)
	currIndex := int64(0)
	genesisIndex := blockSequence[0].BlockIdentifier.Index

	t.Run("No block exists", func(t *testing.T) {
		assertNextSyncableRange(ctx, t, syncer, currIndex)

		// Add genesis block
		balanceChanges, newIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[0],
		)
		currIndex = newIndex
		assert.False(t, reorg)
		assert.Equal(t, int64(1), currIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.NoError(t, err)

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.Equal(t, blockSequence[0].BlockIdentifier, head)
		assert.NoError(t, err)

		assertNextSyncableRange(ctx, t, syncer, currIndex)
	})

	t.Run("Orphan genesis", func(t *testing.T) {
		balanceChanges, newIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			orphanGenesis,
		)

		assert.False(t, reorg)
		assert.Equal(t, int64(0), newIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.EqualError(t, err, "cannot orphan genesis block")

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.Equal(t, blockSequence[0].BlockIdentifier, head)
		assert.NoError(t, err)
	})

	t.Run("Block exists, no reorg", func(t *testing.T) {
		balanceChanges, newIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[1],
		)
		currIndex = newIndex
		assert.False(t, reorg)
		assert.Equal(t, int64(2), currIndex)
		assert.Equal(t, []*storage.BalanceChange{
			{
				Account: &types.AccountIdentifier{
					Address: "acct1",
				},
				Currency:   currency,
				Block:      blockSequence[1].BlockIdentifier,
				Difference: "100",
			},
		}, balanceChanges)
		assert.NoError(t, err)

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		assert.Equal(t, blockSequence[1].BlockIdentifier, head)
		assert.NoError(t, err)

		amounts, block, err := syncer.storage.GetBalance(ctx, tx, recipient)
		tx.Discard(ctx)
		assert.Equal(t, map[string]*types.Amount{
			storage.GetCurrencyKey(currency): recipientAmount,
		}, amounts)
		assert.Equal(t, blockSequence[1].BlockIdentifier, block)
		assert.NoError(t, err)

		assertNextSyncableRange(ctx, t, syncer, currIndex)
	})

	t.Run("Orphan block", func(t *testing.T) {
		// Orphan block
		balanceChanges, newIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[2],
		)
		currIndex = newIndex
		assert.True(t, reorg)
		assert.Equal(t, int64(1), currIndex)
		assert.Equal(t, []*storage.BalanceChange{
			{
				Account: &types.AccountIdentifier{
					Address: "acct1",
				},
				Currency:   currency,
				Block:      blockSequence[0].BlockIdentifier,
				Difference: "-100",
			},
		}, balanceChanges)
		assert.NoError(t, err)
		assertNextSyncableRange(ctx, t, syncer, currIndex)

		// Assert head is back to genesis
		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		assert.Equal(t, blockSequence[0].BlockIdentifier, head)
		assert.NoError(t, err)

		// Assert that balance change was reverted
		// only by the successful operation
		zeroAmount := map[string]*types.Amount{
			storage.GetCurrencyKey(currency): {
				Value:    "0",
				Currency: currency,
			},
		}
		amounts, block, err := syncer.storage.GetBalance(ctx, tx, recipient)
		assert.Equal(t, zeroAmount, amounts)
		assert.Equal(t, blockSequence[0].BlockIdentifier, block)
		assert.NoError(t, err)

		// Assert block is gone
		orphanBlock, err := syncer.storage.GetBlock(ctx, tx, blockSequence[1].BlockIdentifier)
		assert.Nil(t, orphanBlock)
		assert.EqualError(t, err, fmt.Errorf(
			"%w %+v",
			storage.ErrBlockNotFound,
			blockSequence[1].BlockIdentifier,
		).Error())
		tx.Discard(ctx)

		// Process new block
		balanceChanges, currIndex, reorg, err = syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[3],
		)
		assert.False(t, reorg)
		assert.Equal(t, int64(2), currIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.NoError(t, err)
		assertNextSyncableRange(ctx, t, syncer, currIndex)

		tx = syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err = syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.Equal(t, blockSequence[3].BlockIdentifier, head)
		assert.NoError(t, err)

		balanceChanges, currIndex, reorg, err = syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[2],
		)
		assert.False(t, reorg)
		assert.Equal(t, int64(3), currIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.NoError(t, err)

		tx = syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err = syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		assert.Equal(t, blockSequence[2].BlockIdentifier, head)
		assert.NoError(t, err)

		amounts, block, err = syncer.storage.GetBalance(ctx, tx, recipient)
		tx.Discard(ctx)
		assert.Equal(t, zeroAmount, amounts)
		assert.Equal(t, blockSequence[0].BlockIdentifier, block)
		assert.NoError(t, err)
	})

	t.Run("Block with invalid transaction", func(t *testing.T) {
		balanceChanges, currIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[4],
		)
		assert.False(t, reorg)
		assert.Equal(t, int64(3), currIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.Contains(t, err.Error(), storage.ErrNegativeBalance.Error())

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, blockSequence[2].BlockIdentifier, head)
	})

	t.Run("Out of order block", func(t *testing.T) {
		balanceChanges, newIndex, reorg, err := syncer.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			blockSequence[5],
		)
		currIndex = newIndex
		assert.False(t, reorg)
		assert.Equal(t, int64(3), currIndex)
		assert.Equal(t, 0, len(balanceChanges))
		assert.EqualError(t, err, "Got block 5 instead of 3")

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.Equal(t, blockSequence[2].BlockIdentifier, head)
		assert.NoError(t, err)
	})

	t.Run("Revert all blocks after genesis", func(t *testing.T) {
		err := syncer.NewHeadIndex(ctx, genesisIndex)
		assert.NoError(t, err)

		tx := syncer.storage.NewDatabaseTransaction(ctx, false)
		head, err := syncer.storage.GetHeadBlockIdentifier(ctx, tx)
		tx.Discard(ctx)
		assert.Equal(t, blockSequence[0].BlockIdentifier, head)
		assert.NoError(t, err)
	})
}

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

func TestCalculateBalanceChanges(t *testing.T) {
	var tests = map[string]struct {
		block   *types.Block
		orphan  bool
		changes []*storage.BalanceChange
		err     error
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
	asserter, err := asserter.NewWithResponses(
		ctx,
		networkStatusResponse,
		networkOptionsResponse,
	)
	assert.NotNil(t, asserter)
	assert.NoError(t, err)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			changes, err := calculateBalanceChanges(
				ctx,
				asserter,
				test.block,
				test.orphan,
			)

			assert.Equal(t, test.changes, changes)
			assert.Equal(t, test.err, err)
		})
	}
}
