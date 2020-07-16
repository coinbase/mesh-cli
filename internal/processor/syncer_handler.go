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
	"math/big"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-sdk-go/reconciler"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SyncerHandler implements the syncer.Handler interface.
type SyncerHandler struct {
	blockStorage       *storage.BlockStorage
	logger             *logger.Logger
	reconciler         *reconciler.Reconciler
	fetcher            *fetcher.Fetcher
	interestingAccount *reconciler.AccountCurrency
}

// NewSyncerHandler returns a new SyncerHandler.
func NewSyncerHandler(
	blockStorage *storage.BlockStorage,
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
	fetcher *fetcher.Fetcher,
	interestingAccount *reconciler.AccountCurrency,
) *SyncerHandler {
	return &SyncerHandler{
		blockStorage:       blockStorage,
		logger:             logger,
		reconciler:         reconciler,
		fetcher:            fetcher,
		interestingAccount: interestingAccount,
	}
}

// BlockAdded is called by the syncer after a
// block is added.
func (h *SyncerHandler) BlockAdded(
	ctx context.Context,
	block *types.Block,
) error {
	// Log processed blocks and balance changes
	if err := h.logger.AddBlockStream(ctx, block); err != nil {
		return nil
	}

	balanceChanges, err := h.blockStorage.StoreBlock(ctx, block)
	if err != nil {
		return err
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// When an interesting account is provided, only reconcile
	// balance changes affecting that account. This makes finding missing
	// ops much faster.
	if h.interestingAccount != nil {
		var interestingChange *parser.BalanceChange
		for _, change := range balanceChanges {
			if types.Hash(&reconciler.AccountCurrency{
				Account:  change.Account,
				Currency: change.Currency,
			}) == types.Hash(h.interestingAccount) {
				interestingChange = change
				break
			}
		}

		if interestingChange != nil {
			balanceChanges = []*parser.BalanceChange{interestingChange}
		} else {
			balanceChanges = []*parser.BalanceChange{}
		}
	}

	// Update Counters
	_, _ = h.logger.CounterStorage.Update(ctx, storage.BlockCounter, big.NewInt(1))
	_, _ = h.logger.CounterStorage.Update(
		ctx,
		storage.TransactionCounter,
		big.NewInt(int64(len(block.Transactions))),
	)
	opCount := int64(0)
	for _, txn := range block.Transactions {
		opCount += int64(len(txn.Operations))
	}
	_, _ = h.logger.CounterStorage.Update(ctx, storage.OperationCounter, big.NewInt(opCount))

	// Mark accounts for reconciliation...this may be
	// blocking
	return h.reconciler.QueueChanges(ctx, block.BlockIdentifier, balanceChanges)
}

// BlockRemoved is called by the syncer after a
// block is removed.
func (h *SyncerHandler) BlockRemoved(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) error {
	// Log processed blocks and balance changes
	if err := h.logger.RemoveBlockStream(ctx, blockIdentifier); err != nil {
		return nil
	}

	balanceChanges, err := h.blockStorage.RemoveBlock(ctx, blockIdentifier)
	if err != nil {
		return err
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// Update Counters
	_, _ = h.logger.CounterStorage.Update(ctx, storage.OrphanCounter, big.NewInt(1))

	// We only attempt to reconciler changes when blocks are added,
	// not removed
	return nil
}
