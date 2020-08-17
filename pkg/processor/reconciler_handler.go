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
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-cli/pkg/logger"

	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	// ErrReconciliationFailure is returned if reconciliation fails.
	ErrReconciliationFailure = errors.New("reconciliation failure")
)

// ReconcilerHandler implements the Reconciler.Handler interface.
type ReconcilerHandler struct {
	logger                    *logger.Logger
	balanceStorage            *storage.BalanceStorage
	haltOnReconciliationError bool

	InactiveFailure      *reconciler.AccountCurrency
	InactiveFailureBlock *types.BlockIdentifier

	ActiveFailureBlock *types.BlockIdentifier
}

// NewReconcilerHandler creates a new ReconcilerHandler.
func NewReconcilerHandler(
	logger *logger.Logger,
	balanceStorage *storage.BalanceStorage,
	haltOnReconciliationError bool,
) *ReconcilerHandler {
	return &ReconcilerHandler{
		logger:                    logger,
		balanceStorage:            balanceStorage,
		haltOnReconciliationError: haltOnReconciliationError,
	}
}

// ReconciliationFailed is called each time a reconciliation fails.
// In this Handler implementation, we halt if haltOnReconciliationError
// was set to true. We also cancel the context.
func (h *ReconcilerHandler) ReconciliationFailed(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	computedBalance string,
	nodeBalance string,
	block *types.BlockIdentifier,
) error {
	err := h.logger.ReconcileFailureStream(
		ctx,
		reconciliationType,
		account,
		currency,
		computedBalance,
		nodeBalance,
		block,
	)
	if err != nil {
		return err
	}

	if h.haltOnReconciliationError {
		if reconciliationType == reconciler.InactiveReconciliation {
			// Populate inactive failure information so we can try to find block with
			// missing ops.
			h.InactiveFailure = &reconciler.AccountCurrency{
				Account:  account,
				Currency: currency,
			}
			h.InactiveFailureBlock = block
			return fmt.Errorf(
				"%w: inactive reconciliation error for %s at %d (computed: %s%s, live: %s%s)",
				ErrReconciliationFailure,
				account.Address,
				block.Index,
				computedBalance,
				currency.Symbol,
				nodeBalance,
				currency.Symbol,
			)
		}

		// If we halt on an active reconciliation error, store in the handler.
		h.ActiveFailureBlock = block
		return fmt.Errorf(
			"%w: active reconciliation error for %s at %d (computed: %s%s, live: %s%s)",
			ErrReconciliationFailure,
			account.Address,
			block.Index,
			computedBalance,
			currency.Symbol,
			nodeBalance,
			currency.Symbol,
		)
	}

	return nil
}

// ReconciliationSucceeded is called each time a reconciliation succeeds.
func (h *ReconcilerHandler) ReconciliationSucceeded(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	balance string,
	block *types.BlockIdentifier,
) error {
	// Update counters
	if reconciliationType == reconciler.InactiveReconciliation {
		_, _ = h.logger.CounterStorage.Update(
			ctx,
			storage.InactiveReconciliationCounter,
			big.NewInt(1),
		)
	} else {
		_, _ = h.logger.CounterStorage.Update(ctx, storage.ActiveReconciliationCounter, big.NewInt(1))
	}

	if err := h.balanceStorage.Reconciled(ctx, account, currency, block); err != nil {
		return fmt.Errorf("%w: unable to store updated reconciliation", err)
	}

	return h.logger.ReconcileSuccessStream(
		ctx,
		reconciliationType,
		account,
		currency,
		balance,
		block,
	)
}
