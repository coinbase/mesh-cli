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

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	updateFrequency = 10 * time.Second
)

var _ reconciler.Handler = (*ReconcilerHandler)(nil)

var (
	countKeys = []string{
		modules.FailedReconciliationCounter,
		modules.SkippedReconciliationsCounter,
		modules.ExemptReconciliationCounter,
		modules.ActiveReconciliationCounter,
		modules.InactiveReconciliationCounter,
	}
)

// ReconcilerHandler implements the Reconciler.Handler interface.
type ReconcilerHandler struct {
	logger                    *logger.Logger
	counterStorage            *modules.CounterStorage
	balanceStorage            *modules.BalanceStorage
	haltOnReconciliationError bool

	InactiveFailure      *types.AccountCurrency
	InactiveFailureBlock *types.BlockIdentifier

	ActiveFailureBlock *types.BlockIdentifier

	counterLock sync.Mutex
	counts      map[string]int64
}

// NewReconcilerHandler creates a new ReconcilerHandler.
func NewReconcilerHandler(
	logger *logger.Logger,
	counterStorage *modules.CounterStorage,
	balanceStorage *modules.BalanceStorage,
	haltOnReconciliationError bool,
) *ReconcilerHandler {
	counts := map[string]int64{}
	for _, key := range countKeys {
		counts[key] = 0
	}

	return &ReconcilerHandler{
		logger:                    logger,
		counterStorage:            counterStorage,
		balanceStorage:            balanceStorage,
		haltOnReconciliationError: haltOnReconciliationError,
		counts:                    counts,
	}
}

// Updater periodically updates modules.with cached counts.
func (h *ReconcilerHandler) Updater(ctx context.Context) error {
	tc := time.NewTicker(updateFrequency)
	defer tc.Stop()

	for {
		select {
		case <-tc.C:
			if err := h.UpdateCounts(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// UpdateCounts forces cached counts to be written to modules.
func (h *ReconcilerHandler) UpdateCounts(ctx context.Context) error {
	for _, key := range countKeys {
		h.counterLock.Lock()
		count := h.counts[key]
		h.counts[key] = 0
		h.counterLock.Unlock()

		if count == 0 {
			continue
		}

		if _, err := h.counterStorage.Update(ctx, key, big.NewInt(count)); err != nil {
			return fmt.Errorf("failed to key %s in counter storage: %w", key, err)
		}
	}

	return nil
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
	h.counts[modules.FailedReconciliationCounter]++
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
		return fmt.Errorf("failed to log reconciliation checks when reconciliation is failed: %w", err)
	}

	if h.haltOnReconciliationError {
		// Update counts before exiting
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
				"inactive reconciliation error for account address %s at block index %d (computed: %s%s, live: %s%s): %w",
				account.Address,
				block.Index,
				computedBalance,
				currency.Symbol,
				liveBalance,
				currency.Symbol,
				cliErrs.ErrReconciliationFailure,
			)
		}

		// If we halt on an active reconciliation error, store in the handler.
		h.ActiveFailureBlock = block
		return fmt.Errorf(
			"active reconciliation error for account address %s at block index %d (computed: %s%s, live: %s%s): %w",
			account.Address,
			block.Index,
			computedBalance,
			currency.Symbol,
			liveBalance,
			currency.Symbol,
			cliErrs.ErrReconciliationFailure,
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
	h.counts[modules.ExemptReconciliationCounter]++
	h.counterLock.Unlock()

	// Although the reconciliation was exempt (non-zero difference that was ignored),
	// we still mark the account as being reconciled because the balance was in the range
	// specified by exemption.
	if err := h.balanceStorage.Reconciled(ctx, account, currency, block); err != nil {
		return fmt.Errorf("unable to store updated reconciliation currency %s of account %s at block %s: %w", types.PrintStruct(currency), types.PrintStruct(account), types.PrintStruct(block), err)
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
	h.counts[modules.SkippedReconciliationsCounter]++
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
	// Update counters
	counter := modules.ActiveReconciliationCounter
	if reconciliationType == reconciler.InactiveReconciliation {
		counter = modules.InactiveReconciliationCounter
	}

	h.counterLock.Lock()
	h.counts[counter]++
	h.counterLock.Unlock()

	if err := h.balanceStorage.Reconciled(ctx, account, currency, block); err != nil {
		return fmt.Errorf("unable to store updated reconciliation currency %s of account %s at block %s: %w", types.PrintStruct(currency), types.PrintStruct(account), types.PrintStruct(block), err)
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
