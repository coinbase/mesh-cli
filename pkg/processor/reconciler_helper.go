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
	"errors"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// ReconcilerHelper implements the Reconciler.Helper
// interface.
type ReconcilerHelper struct {
	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher

	blockStorage   *storage.BlockStorage
	balanceStorage *storage.BalanceStorage
}

// NewReconcilerHelper returns a new ReconcilerHelper.
func NewReconcilerHelper(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	blockStorage *storage.BlockStorage,
	balanceStorage *storage.BalanceStorage,
) *ReconcilerHelper {
	return &ReconcilerHelper{
		network:        network,
		fetcher:        fetcher,
		blockStorage:   blockStorage,
		balanceStorage: balanceStorage,
	}
}

// BlockExists returns a boolean indicating if block_storage
// contains a block. This is necessary to reconcile across
// reorgs. If the block returned on an account balance fetch
// does not exist, reconciliation will be skipped.
func (h *ReconcilerHelper) BlockExists(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	_, err := h.blockStorage.GetBlock(ctx, types.ConstructPartialBlockIdentifier(block))
	if err == nil {
		return true, nil
	}

	if errors.Is(err, storage.ErrBlockNotFound) {
		return false, nil
	}

	return false, err
}

// CurrentBlock returns the last processed block and is used
// to determine which block to check account balances at during
// inactive reconciliation.
func (h *ReconcilerHelper) CurrentBlock(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return h.blockStorage.GetHeadBlockIdentifier(ctx)
}

// ComputedBalance returns the balance of an account in block storage.
// It is necessary to perform this check outside of the Reconciler
// package to allow for separation from a default storage backend.
func (h *ReconcilerHelper) ComputedBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	return h.balanceStorage.GetBalance(ctx, account, currency, headBlock)
}

// LiveBalance returns the live balance of an account.
func (h *ReconcilerHelper) LiveBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	amt, block, _, err := utils.CurrencyBalance(
		ctx,
		h.network,
		h.fetcher,
		account,
		currency,
		headBlock,
	)
	if err != nil {
		return nil, nil, err
	}
	return amt, block, nil
}
