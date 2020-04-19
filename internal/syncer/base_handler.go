package syncer

import (
	"context"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// BaseHandler logs processed blocks
// and reconciles modified balances.
type BaseHandler struct {
	logger     *logger.Logger
	reconciler *reconciler.Reconciler
}

// NewBaseHandler constructs a basic Handler.
func NewBaseHandler(
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
) Handler {
	return &BaseHandler{
		logger:     logger,
		reconciler: reconciler,
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
	// Log processed blocks and balance changes
	if err := h.logger.BlockStream(ctx, block, reorg); err != nil {
		return nil
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// Mark accounts for reconciliation
	h.reconciler.QueueAccounts(ctx, block.BlockIdentifier.Index, balanceChanges)

	return nil
}
