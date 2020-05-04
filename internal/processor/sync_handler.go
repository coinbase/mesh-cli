package processor

import (
	"context"
	"log"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

type SyncHandler struct {
	storage    *storage.BlockStorage
	logger     *logger.Logger
	reconciler *reconciler.Reconciler
	fetcher    *fetcher.Fetcher

	exemptAccounts []*reconciler.AccountCurrency
}

func NewSyncHandler(
	storage *storage.BlockStorage,
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
	fetcher *fetcher.Fetcher,
	exemptAccounts []*reconciler.AccountCurrency,
) *SyncHandler {
	return &SyncHandler{
		storage:        storage,
		logger:         logger,
		reconciler:     reconciler,
		fetcher:        fetcher,
		exemptAccounts: exemptAccounts,
	}
}

// BlockAdded is called by the syncer after a
// block is added.
func (h *SyncHandler) BlockAdded(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Adding block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := h.logger.BlockStream(ctx, block, false); err != nil {
		return nil
	}

	balanceChanges, err := h.storage.StoreBlock(ctx, block)
	if err != nil {
		return err
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// Mark accounts for reconciliation...this may be
	// blocking
	return h.reconciler.QueueChanges(ctx, block.BlockIdentifier, balanceChanges)
}

// BlockRemoved is called by the syncer after a
// block is removed.
func (h *SyncHandler) BlockRemoved(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Orphaning block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := h.logger.BlockStream(ctx, block, true); err != nil {
		return nil
	}

	balanceChanges, err := h.storage.RemoveBlock(ctx, block)
	if err != nil {
		return err
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// We only attempt to reconciler changes when blocks are added,
	// not removed
	return nil
}
