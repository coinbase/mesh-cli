package processor

import (
	"context"
	"errors"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type ReconcilerHelper struct{}

func (h *ReconcilerHelper) BlockExists(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	return false, errors.New("not implemented")
}

func (h *ReconcilerHelper) CurrentBlock(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return nil, errors.New("not implemented")
}

func (h *ReconcilerHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
) (*types.Amount, *types.BlockIdentifier, error) {
	return nil, nil, errors.New("not implemented")
}
