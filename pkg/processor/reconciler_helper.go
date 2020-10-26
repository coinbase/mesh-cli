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

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ reconciler.Helper = (*ReconcilerHelper)(nil)

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

// CanonicalBlock returns a boolean indicating if a block
// is in the canonical chain. This is necessary to reconcile across
// reorgs. If the block returned on an account balance fetch
// does not exist, reconciliation will be skipped.
func (h *ReconcilerHelper) CanonicalBlock(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	return h.blockStorage.CanonicalBlock(ctx, block)
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
) (*types.Amount, error) {
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

// PruneBalances removes all historical balance states
// <= some index. This can significantly reduce storage
// usage in scenarios where historical balances are only
// retrieved once (like reconciliation).
func (h *ReconcilerHelper) PruneBalances(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) error {
	return h.balanceStorage.PruneBalances(
		ctx,
		account,
		currency,
		index,
	)
}
