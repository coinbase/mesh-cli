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

	"github.com/coinbase/rosetta-cli/internal/logger"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ReconcilerHandler implements the Reconciler.Handler interface.
type ReconcilerHandler struct {
	cancel                    context.CancelFunc
	logger                    *logger.Logger
	haltOnReconciliationError bool
}

// NewReconcilerHandler creates a new ReconcilerHandler.
func NewReconcilerHandler(
	cancel context.CancelFunc,
	logger *logger.Logger,
	haltOnReconciliationError bool,
) *ReconcilerHandler {
	return &ReconcilerHandler{
		cancel:                    cancel,
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
		h.cancel()
		return errors.New("halting due to reconciliation error")
	}

	// TODO: automatically find block with missing operation
	// if inactive reconciliation error. Can do this by asserting
	// the impacted address has a balance change of 0 on all blocks
	// it is not active.
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
	return h.logger.ReconcileSuccessStream(
		ctx,
		reconciliationType,
		account,
		currency,
		balance,
		block,
	)
}

// NewAccountSeen is called each time the reconciler adds a new
// AccountCurrency to the inactiveQueue. These AccountCurrency
// should be persisted to pass to the reconciler on restart.
func (h *ReconcilerHandler) NewAccountSeen(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
) error {
	fmt.Printf("New Account Seen: %s\n", types.PrettyPrintStruct(account))
	// TODO: store somewhere...should be append only otherwise we will need to
	// load arbitrarily large structures into memory for each addition.
	return nil
}
