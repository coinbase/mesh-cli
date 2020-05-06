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

	"github.com/coinbase/rosetta-cli/internal/logger"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type ReconcilerHandler struct {
	cancel                    context.CancelFunc
	logger                    *logger.Logger
	haltOnReconciliationError bool
}

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
