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

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ reconciler.Helper = (*ReconcilerHelper)(nil)

// ReconcilerHelper implements the Reconciler.Helper
// interface.
type ReconcilerHelper struct {
	config *configuration.Configuration

	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher

	database                    database.Database
	blockStorage                *modules.BlockStorage
	balanceStorage              *modules.BalanceStorage
	forceInactiveReconciliation *bool
}

// NewReconcilerHelper returns a new ReconcilerHelper.
func NewReconcilerHelper(
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	database database.Database,
	blockStorage *modules.BlockStorage,
	balanceStorage *modules.BalanceStorage,
	forceInactiveReconciliation *bool,
) *ReconcilerHelper {
	return &ReconcilerHelper{
		config:                      config,
		network:                     network,
		fetcher:                     fetcher,
		database:                    database,
		blockStorage:                blockStorage,
		balanceStorage:              balanceStorage,
		forceInactiveReconciliation: forceInactiveReconciliation,
	}
}

// DatabaseTransaction returns a new read-only database.Transaction.
func (h *ReconcilerHelper) DatabaseTransaction(
	ctx context.Context,
) database.Transaction {
	return h.database.ReadTransaction(ctx)
}

// CanonicalBlock returns a boolean indicating if a block
// is in the canonical chain. This is necessary to reconcile across
// reorgs. If the block returned on an account balance fetch
// does not exist, reconciliation will be skipped.
func (h *ReconcilerHelper) CanonicalBlock(
	ctx context.Context,
	dbTx database.Transaction,
	block *types.BlockIdentifier,
) (bool, error) {
	return h.blockStorage.CanonicalBlockTransactional(ctx, block, dbTx)
}

// IndexAtTip returns a boolean indicating if a block
// index is at tip (provided some acceptable
// tip delay). If the index is ahead of the head block
// and the head block is at tip, we consider the
// index at tip.
func (h *ReconcilerHelper) IndexAtTip(
	ctx context.Context,
	index int64,
) (bool, error) {
	return h.blockStorage.IndexAtTip(
		ctx,
		h.config.TipDelay,
		index,
	)
}

// CurrentBlock returns the last processed block and is used
// to determine which block to check account balances at during
// inactive reconciliation.
func (h *ReconcilerHelper) CurrentBlock(
	ctx context.Context,
	dbTx database.Transaction,
) (*types.BlockIdentifier, error) {
	return h.blockStorage.GetHeadBlockIdentifierTransactional(ctx, dbTx)
}

// ComputedBalance returns the balance of an account in block storage.
// It is necessary to perform this check outside of the Reconciler
// package to allow for separation from a default storage backend.
func (h *ReconcilerHelper) ComputedBalance(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	return h.balanceStorage.GetBalanceTransactional(ctx, dbTx, account, currency, index)
}

// LiveBalance returns the live balance of an account.
func (h *ReconcilerHelper) LiveBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, *types.BlockIdentifier, error) {
	amt, block, err := utils.CurrencyBalance(
		ctx,
		h.network,
		h.fetcher,
		account,
		currency,
		index,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get current balance of currency %s of account %s: %w", types.PrintStruct(currency), types.PrintStruct(account), err)
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
	if h.config.Data.PruningBalanceDisabled {
		return nil
	}

	return h.balanceStorage.PruneBalances(
		ctx,
		account,
		currency,
		index,
	)
}

// ForceInactiveReconciliation overrides the default
// calculation to determine if an account should be
// reconciled inactively.
func (h *ReconcilerHelper) ForceInactiveReconciliation(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	lastChecked *types.BlockIdentifier,
) bool {
	if h.forceInactiveReconciliation == nil {
		return false
	}

	return *h.forceInactiveReconciliation
}
