package processor

import (
	"context"
	"math/big"

	"github.com/coinbase/rosetta-cli/internal/constructor"
	"github.com/coinbase/rosetta-cli/internal/storage"
)

var _ constructor.ConstructorHandler = (*ConstructorHandler)(nil)

type ConstructorHandler struct {
	balanceStorageHelper *BalanceStorageHelper

	counterStorage *storage.CounterStorage
}

func (h *ConstructorHandler) AddressCreated(ctx context.Context, address string) error {
	h.balanceStorageHelper.AddInterestingAddress(address)

	_, _ = h.counterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return nil
}
