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
	"math/big"
	"sync"
	"time"

	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/results"

	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ reconciler.Handler = (*ReconcilerHandler)(nil)

// ReconcilerHandler implements the Reconciler.Handler interface.
type ReconcilerHandler struct {
	logger                    *logger.Logger
	counterStorage            *storage.CounterStorage
	balanceStorage            *storage.BalanceStorage
	haltOnReconciliationError bool

	InactiveFailure      *types.AccountCurrency
	InactiveFailureBlock *types.BlockIdentifier

	ActiveFailureBlock *types.BlockIdentifier

	// Batch counter updates
	counterLock sync.Mutex
	skipped     int64
	failed      int64
	exempt      int64
	active      int64
	inactive    int64
}

// NewReconcilerHandler creates a new ReconcilerHandler.
func NewReconcilerHandler(
	logger *logger.Logger,
	counterStorage *storage.CounterStorage,
	balanceStorage *storage.BalanceStorage,
	haltOnReconciliationError bool,
) *ReconcilerHandler {
	return &ReconcilerHandler{
		logger:                    logger,
		counterStorage:            counterStorage,
		balanceStorage:            balanceStorage,
		haltOnReconciliationError: haltOnReconciliationError,
	}
}

func (h *ReconcilerHandler) UpdateCounts(ctx context.Context) error {
	var failed, skipped, exempt, active, inactive int64
	h.counterLock.Lock()
	failed = h.failed
	h.failed = 0
	skipped = h.skipped
	h.skipped = 0
	exempt = h.exempt
	h.exempt = 0
	active = h.active
	h.active = 0
	inactive = h.inactive
	h.inactive = 0
	h.counterLock.Unlock()

	if failed > 0 {
		if _, err := h.counterStorage.Update(
			ctx,
			storage.FailedReconciliationCounter,
			big.NewInt(failed),
		); err != nil {
			return err
		}
	}

	if skipped > 0 {
		if _, err := h.counterStorage.Update(
			ctx,
			storage.SkippedReconciliationsCounter,
			big.NewInt(skipped),
		); err != nil {
			return err
		}
	}

	if exempt > 0 {
		if _, err := h.counterStorage.Update(
			ctx,
			storage.ExemptReconciliationCounter,
			big.NewInt(exempt),
		); err != nil {
			return err
		}
	}

	if active > 0 {
		if _, err := h.counterStorage.Update(
			ctx,
			storage.ActiveReconciliationCounter,
			big.NewInt(active),
		); err != nil {
			return err
		}
	}

	if inactive > 0 {
		if _, err := h.counterStorage.Update(
			ctx,
			storage.InactiveReconciliationCounter,
			big.NewInt(inactive),
		); err != nil {
			return err
		}
	}

	return nil
}

// TODO: add batched counter update (run every 10 seconds)
func (h *ReconcilerHandler) Updater(ctx context.Context) error {
	tc := time.NewTicker(10 * time.Second)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tc.C:
			if err := h.UpdateCounts(ctx); err != nil {
				return err
			}
		}
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
	liveBalance string,
	block *types.BlockIdentifier,
) error {
	h.counterLock.Lock()
	h.failed++
	h.counterLock.Unlock()

	err := h.logger.ReconcileFailureStream(
		ctx,
		reconciliationType,
		account,
		currency,
		computedBalance,
		liveBalance,
		block,
	)
	if err != nil {
		return err
	}

	if h.haltOnReconciliationError {
		// update counts before exiting
		_ = h.UpdateCounts(ctx)

		if reconciliationType == reconciler.InactiveReconciliation {
			// Populate inactive failure information so we can try to find block with
			// missing ops.
			h.InactiveFailure = &types.AccountCurrency{
				Account:  account,
				Currency: currency,
			}
			h.InactiveFailureBlock = block
			return fmt.Errorf(
				"%w: inactive reconciliation error for %s at %d (computed: %s%s, live: %s%s)",
				results.ErrReconciliationFailure,
				account.Address,
				block.Index,
				computedBalance,
				currency.Symbol,
				liveBalance,
				currency.Symbol,
			)
		}

		// If we halt on an active reconciliation error, store in the handler.
		h.ActiveFailureBlock = block
		return fmt.Errorf(
			"%w: active reconciliation error for %s at %d (computed: %s%s, live: %s%s)",
			results.ErrReconciliationFailure,
			account.Address,
			block.Index,
			computedBalance,
			currency.Symbol,
			liveBalance,
			currency.Symbol,
		)
	}

	return nil
}

// ReconciliationExempt is called each time a reconciliation fails
// but is considered exempt because of provided []*types.BalanceExemption.
func (h *ReconcilerHandler) ReconciliationExempt(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	computedBalance string,
	liveBalance string,
	block *types.BlockIdentifier,
	exemption *types.BalanceExemption,
) error {
	h.counterLock.Lock()
	h.exempt++
	h.counterLock.Unlock()

	// Although the reconciliation was exempt (non-zero difference that was ignored),
	// we still mark the account as being reconciled because the balance was in the range
	// specified by exemption.
	if err := h.balanceStorage.Reconciled(ctx, account, currency, block); err != nil {
		return fmt.Errorf("%w: unable to store updated reconciliation", err)
	}

	return nil
}

// ReconciliationSkipped is called each time a reconciliation is skipped.
func (h *ReconcilerHandler) ReconciliationSkipped(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	cause string,
) error {
	h.counterLock.Lock()
	h.skipped++
	h.counterLock.Unlock()

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
	start := time.Now()

	// Update counters
	h.counterLock.Lock()
	if reconciliationType == reconciler.InactiveReconciliation {
		h.inactive++
	} else {
		h.active++
	}
	h.counterLock.Unlock()

	if err := h.balanceStorage.Reconciled(ctx, account, currency, block); err != nil {
		return fmt.Errorf("%w: unable to store updated reconciliation", err)
	}

	fmt.Println("reconciliation detail", time.Since(start))

	return h.logger.ReconcileSuccessStream(
		ctx,
		reconciliationType,
		account,
		currency,
		balance,
		block,
	)
}
