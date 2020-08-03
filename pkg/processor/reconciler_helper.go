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

	"github.com/slowboat0/rosetta-cli/pkg/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ReconcilerHelper implements the Reconciler.Helper
// interface.
type ReconcilerHelper struct {
	blockStorage   *storage.BlockStorage
	balanceStorage *storage.BalanceStorage
}

// NewReconcilerHelper returns a new ReconcilerHelper.
func NewReconcilerHelper(
	blockStorage *storage.BlockStorage,
	balanceStorage *storage.BalanceStorage,
) *ReconcilerHelper {
	return &ReconcilerHelper{
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
	_, err := h.blockStorage.GetBlock(ctx, block)
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

// AccountBalance returns the balance of an account in block storage.
// It is necessary to perform this check outside of the Reconciler
// package to allow for separation from a default storage backend.
func (h *ReconcilerHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	return h.balanceStorage.GetBalance(ctx, account, currency, headBlock)
}
