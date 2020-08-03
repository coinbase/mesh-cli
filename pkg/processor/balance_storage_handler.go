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

	"github.com/slowboat0/rosetta-cli/pkg/logger"
	"github.com/slowboat0/rosetta-cli/pkg/storage"

	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ storage.BalanceStorageHandler = (*BalanceStorageHandler)(nil)

// BalanceStorageHandler is invoked whenever a block is added
// or removed from block storage so that balance changes
// can be sent to other functions (ex: reconciler).
type BalanceStorageHandler struct {
	logger     *logger.Logger
	reconciler *reconciler.Reconciler

	reconcile          bool
	interestingAccount *reconciler.AccountCurrency
}

// NewBalanceStorageHandler returns a new *BalanceStorageHandler.
func NewBalanceStorageHandler(
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
	reconcile bool,
	interestingAccount *reconciler.AccountCurrency,
) *BalanceStorageHandler {
	return &BalanceStorageHandler{
		logger:             logger,
		reconciler:         reconciler,
		reconcile:          reconcile,
		interestingAccount: interestingAccount,
	}
}

// BlockAdded is called whenever a block is committed to BlockStorage.
func (h *BalanceStorageHandler) BlockAdded(
	ctx context.Context,
	block *types.Block,
	changes []*parser.BalanceChange,
) error {
	_ = h.logger.BalanceStream(ctx, changes)

	// When testing, it can be useful to not run any reconciliations to just check
	// if blocks are well formatted and balances don't go negative.
	if !h.reconcile {
		return nil
	}

	// When an interesting account is provided, only reconcile
	// balance changes affecting that account. This makes finding missing
	// ops much faster.
	if h.interestingAccount != nil {
		var interestingChange *parser.BalanceChange
		for _, change := range changes {
			if types.Hash(&reconciler.AccountCurrency{
				Account:  change.Account,
				Currency: change.Currency,
			}) == types.Hash(h.interestingAccount) {
				interestingChange = change
				break
			}
		}

		if interestingChange != nil {
			changes = []*parser.BalanceChange{interestingChange}
		} else {
			changes = []*parser.BalanceChange{}
		}
	}

	// Mark accounts for reconciliation...this may be
	// blocking
	return h.reconciler.QueueChanges(ctx, block.BlockIdentifier, changes)
}

// BlockRemoved is called whenever a block is removed from BlockStorage.
func (h *BalanceStorageHandler) BlockRemoved(
	ctx context.Context,
	block *types.Block,
	changes []*parser.BalanceChange,
) error {
	_ = h.logger.BalanceStream(ctx, changes)

	// We only attempt to reconciler changes when blocks are added,
	// not removed
	return nil
}
