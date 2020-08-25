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

package processor

import (
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ storage.BroadcastStorageHelper = (*BroadcastStorageHelper)(nil)

// BroadcastStorageHelper implements the storage.Helper
// interface.
type BroadcastStorageHelper struct {
	network      *types.NetworkIdentifier
	blockStorage *storage.BlockStorage
	fetcher      *fetcher.Fetcher
}

// NewBroadcastStorageHelper returns a new BroadcastStorageHelper.
func NewBroadcastStorageHelper(
	blockStorage *storage.BlockStorage,
	fetcher *fetcher.Fetcher,
) *BroadcastStorageHelper {
	return &BroadcastStorageHelper{
		blockStorage: blockStorage,
		fetcher:      fetcher,
	}
}

// AtTip is called before transaction broadcast to determine if we are at tip.
func (h *BroadcastStorageHelper) AtTip(
	ctx context.Context,
	tipDelay int64,
) (bool, error) {
	atTip, _, err := h.blockStorage.AtTip(ctx, tipDelay)
	if err != nil {
		return false, fmt.Errorf("%w: unable to determine if at tip", err)
	}

	return atTip, nil
}

// CurrentBlockIdentifier is called before transaction broadcast and is used
// to determine if a transaction broadcast is stale.
func (h *BroadcastStorageHelper) CurrentBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	blockIdentifier, err := h.blockStorage.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get head block identifier", err)
	}

	return blockIdentifier, nil
}

// FindTransaction looks for the provided TransactionIdentifier in processed
// blocks and returns the block identifier containing the most recent sighting
// and the transaction seen in that block.
func (h *BroadcastStorageHelper) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn storage.DatabaseTransaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	newestBlock, transaction, err := h.blockStorage.FindTransaction(ctx, transactionIdentifier, txn)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to perform transaction search", err)
	}

	return newestBlock, transaction, nil
}

// BroadcastTransaction broadcasts a transaction to a Rosetta implementation
// and returns the *types.TransactionIdentifier returned by the implementation.
func (h *BroadcastStorageHelper) BroadcastTransaction(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	networkTransaction string,
) (*types.TransactionIdentifier, error) {
	transactionIdentifier, _, fetchErr := h.fetcher.ConstructionSubmit(
		ctx,
		networkIdentifier,
		networkTransaction,
	)
	if fetchErr != nil {
		return nil, fmt.Errorf("%w: unable to broadcast transaction", fetchErr.Err)
	}

	return transactionIdentifier, nil
}
