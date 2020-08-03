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
	"math/big"

	"github.com/slowboat0/rosetta-cli/pkg/logger"
	"github.com/slowboat0/rosetta-cli/pkg/storage"

	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// ReconcilerHandler implements the Reconciler.Handler interface.
type ReconcilerHandler struct {
	logger                    *logger.Logger
	haltOnReconciliationError bool

	InactiveFailure      *reconciler.AccountCurrency
	InactiveFailureBlock *types.BlockIdentifier

	ActiveFailureBlock *types.BlockIdentifier
}

// NewReconcilerHandler creates a new ReconcilerHandler.
func NewReconcilerHandler(
	logger *logger.Logger,
	haltOnReconciliationError bool,
) *ReconcilerHandler {
	return &ReconcilerHandler{
		logger:                    logger,
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
		} else {
			// If we halt on an active reconciliation error, store in the handler.
			h.ActiveFailureBlock = block
		}
		return errors.New("reconciliation error")
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

	return h.logger.ReconcileSuccessStream(
		ctx,
		reconciliationType,
		account,
		currency,
		balance,
		block,
	)
}
