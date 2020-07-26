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
	"errors"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ BlockWorker = (*BroadcastStorage)(nil)

// BroadcastStorage implements storage methods for managing
// transaction broadcast.
type BroadcastStorage struct {
	db      Database
	helper  BroadcastStorageHelper
	handler BroadcastStorageHandler

	confirmationDepth int64
	staleDepth        int64
}

type BroadcastStorageHelper interface {
	CurrentBlockIdentifier(context.Context) (*types.BlockIdentifier, error)             // used to determine if should rebroadcast
	FindTransaction(context.Context, *types.TransactionIdentifier) (int64, error)       // used to confirm
	BroadcastTransaction(context.Context, string) (*types.TransactionIdentifier, error) // handle initial broadcast + confirm matches provided + rebroadcast if stale
}

type BroadcastStorageHandler interface {
	TransactionConfirmed(context.Context, *types.Transaction, []*types.Operation) error // can use locked account again + confirm matches intent + update logger
	TransactionRebroadcast(context.Context, *types.TransactionIdentifier) error         // log in counter
}

type Broadcast struct {
	Identifier    *types.TransactionIdentifier `json:"identifier"`
	Intent        []*types.Operation           `json:"intent"`
	LastBroadcast *types.BlockIdentifier       `json:"broadcast_at"`
}

// NewBroadcastStorage returns a new BroadcastStorage.
func NewBroadcastStorage(
	db Database,
	confirmationDepth int64,
	staleDepth int64,
) *BroadcastStorage {
	return &BroadcastStorage{
		db:                db,
		confirmationDepth: confirmationDepth,
		staleDepth:        staleDepth,
	}
}

func (b *BroadcastStorage) Initialize(helper BroadcastStorageHelper, handler BroadcastStorageHandler) {
	b.helper = helper
	b.handler = handler
}

// AddingBlock is called by BlockStorage when adding a block.
func (b *BroadcastStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	// TODO: call handler -> transactionRebroadcast should not block processing (could be in CommitWorker)
	return nil, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
// TODO: error if transaction removed after confirmed (means confirmation depth not deep enough)
func (b *BroadcastStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	return nil, nil
}

func (b *BroadcastStorage) Broadcast(ctx context.Context, transactionIdentifier *types.TransactionIdentifier) error {
	return errors.New("not implemented")
}
func (b *BroadcastStorage) LockedAddresses() error {
	return errors.New("not implemented")
}
