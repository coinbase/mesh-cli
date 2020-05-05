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
