package processor

import (
	"context"
	"errors"

	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type ReconcilerHelper struct {
	storage *storage.BlockStorage
}

func NewReconcilerHelper(
	storage *storage.BlockStorage,
) *ReconcilerHelper {
	return &ReconcilerHelper{
		storage: storage,
	}
}

func (h *ReconcilerHelper) BlockExists(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	_, err := h.storage.GetBlock(ctx, block)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, storage.ErrBlockNotFound) {
		return false, nil
	}

	return false, err
}

func (h *ReconcilerHelper) CurrentBlock(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return h.storage.GetHeadBlockIdentifier(ctx)
}

func (h *ReconcilerHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
) (*types.Amount, *types.BlockIdentifier, error) {
	return h.storage.GetBalance(ctx, account, currency)
}
