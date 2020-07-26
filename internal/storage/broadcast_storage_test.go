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
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

const (
	confirmationDepth = int64(2)
	staleDepth        = int64(1)
	broadcastLimit    = 3
)

func makeFillerBlocks(start int64, end int64) []*types.Block {
	blocks := []*types.Block{}
	for i := start; i < end; i++ {
		parentIndex := i - 1
		if parentIndex < 0 {
			parentIndex = 0
		}
		blocks = append(blocks, &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Index: i,
				Hash:  fmt.Sprintf("block %d", i),
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Index: parentIndex,
				Hash:  fmt.Sprintf("block %d", parentIndex),
			},
		})
	}

	return blocks
}

func opFiller(sender string, opNumber int) []*types.Operation {
	ops := make([]*types.Operation, opNumber)
	for i := 0; i < opNumber; i++ {
		ops[i] = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: int64(i),
			},
			Account: &types.AccountIdentifier{
				Address: sender,
				SubAccount: &types.SubAccountIdentifier{
					Address: sender,
				},
			},
		}
	}

	return ops
}

func TestBroadcastStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBroadcastStorage(database, confirmationDepth, staleDepth, broadcastLimit)
	mockHelper := &MockBroadcastStorageHelper{}
	mockHandler := &MockBroadcastStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	t.Run("locked addresses with no broadcasts", func(t *testing.T) {
		addresses, err := storage.LockedAddresses(ctx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.NotNil(t, addresses)
	})

	t.Run("broadcast", func(t *testing.T) {
		send1 := opFiller("addr 1", 11)
		err := storage.Broadcast(ctx, "addr 1", send1, &types.TransactionIdentifier{Hash: "tx 1"}, "payload 1")
		assert.NoError(t, err)

		send2 := opFiller("addr 2", 13)
		err = storage.Broadcast(ctx, "addr 2", send2, &types.TransactionIdentifier{Hash: "tx 2"}, "payload 2")
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Sender:     "addr 1",
				Intent:     send1,
				Payload:    "payload 1",
			},
			{
				Identifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Sender:     "addr 2",
				Intent:     send2,
				Payload:    "payload 2",
			},
		}, broadcasts)
	})
}

var _ BroadcastStorageHelper = (*MockBroadcastStorageHelper)(nil)

type MockBroadcastStorageHelper struct{}

func (m *MockBroadcastStorageHelper) CurrentRemoteBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return nil, nil
}

func (m *MockBroadcastStorageHelper) CurrentBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return nil, nil
}

func (m *MockBroadcastStorageHelper) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
) (*types.BlockIdentifier, *types.Transaction, error) {
	return nil, nil, nil
}

func (m *MockBroadcastStorageHelper) BroadcastTransaction(
	ctx context.Context,
	payload string,
) (*types.TransactionIdentifier, error) {
	return nil, nil
}

var _ BroadcastStorageHandler = (*MockBroadcastStorageHandler)(nil)

type MockBroadcastStorageHandler struct{}

func (m *MockBroadcastStorageHandler) TransactionConfirmed(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transaction *types.Transaction,
	intent []*types.Operation,
) error {
	return nil
}

func (m *MockBroadcastStorageHandler) TransactionStale(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	return nil
}

func (m *MockBroadcastStorageHandler) BroadcastFailed(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	intent []*types.Operation,
) error {
	return nil
}
