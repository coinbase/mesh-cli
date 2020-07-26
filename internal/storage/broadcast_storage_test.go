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
