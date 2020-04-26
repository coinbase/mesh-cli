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

package syncer

import (
	"context"
	"log"
	"reflect"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// BaseHandler logs processed blocks
// and reconciles modified balances.
type BaseHandler struct {
	logger              *logger.Logger
	reconciler          reconciler.Reconciler
	interestingAccounts []*reconciler.AccountCurrency
}

// NewBaseHandler constructs a basic Handler.
func NewBaseHandler(
	logger *logger.Logger,
	reconciler reconciler.Reconciler,
	interestingAccounts []*reconciler.AccountCurrency,
) Handler {
	return &BaseHandler{
		logger:              logger,
		reconciler:          reconciler,
		interestingAccounts: interestingAccounts,
	}
}

// BlockProcessed is called by the syncer after each
// block is processed.
func (h *BaseHandler) BlockProcessed(
	ctx context.Context,
	block *types.Block,
	reorg bool,
	balanceChanges []*storage.BalanceChange,
) error {
	if !reorg {
		log.Printf("Adding block %+v\n", block.BlockIdentifier)
	} else {
		log.Printf("Orphaning block %+v\n", block.BlockIdentifier)
	}
	// Log processed blocks and balance changes
	if err := h.logger.BlockStream(ctx, block, reorg); err != nil {
		return nil
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// TODO: refactor this once handler is cleaned up to remove storage
	// dependency in syncer
	for _, account := range h.interestingAccounts {
		skipAccount := false
		// Look through balance changes for account + currency
		for _, change := range balanceChanges {
			if reflect.DeepEqual(change.Account, account.Account) && reflect.DeepEqual(change.Currency, account.Currency) {
				skipAccount = true
				break
			}
		}

		// Account changed on this block
		if skipAccount {
			continue
		}

		// If account + currency not found, add with difference 0
		balanceChanges = append(balanceChanges, &storage.BalanceChange{
			Account:    account.Account,
			Currency:   account.Currency,
			Difference: "0",
			Block:      block.BlockIdentifier,
		})
	}

	// Mark accounts for reconciliation...this may be
	// blocking
	return h.reconciler.QueueChanges(ctx, balanceChanges)
}
