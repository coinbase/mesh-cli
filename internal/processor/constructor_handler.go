package processor

import (
	"context"
	"math/big"

	"github.com/coinbase/rosetta-cli/internal/constructor"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ constructor.ConstructorHandler = (*ConstructorHandler)(nil)

type ConstructorHandler struct {
	balanceStorageHelper *BalanceStorageHelper

	counterStorage *storage.CounterStorage
}

func NewConstructorHandler(
	balanceStorageHelper *BalanceStorageHelper,
	counterStorage *storage.CounterStorage,
) *ConstructorHandler {
	return &ConstructorHandler{
		balanceStorageHelper: balanceStorageHelper,
		counterStorage:       counterStorage,
	}
}

func (h *ConstructorHandler) AddressCreated(ctx context.Context, address string) error {
	h.balanceStorageHelper.AddInterestingAddress(address)

	_, _ = h.counterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return nil
}

func (h *ConstructorHandler) TransactionCreated(
	ctx context.Context,
	sender string,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	_, _ = h.counterStorage.Update(
		ctx,
		storage.TransactionsCreatedCounter,
		big.NewInt(1),
	)

	return nil
}
