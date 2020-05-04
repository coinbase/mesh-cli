package processor

import (
	"context"
	"errors"
	"log"

	"github.com/coinbase/rosetta-cli/internal/reconciler"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

type BlockStorageHelper struct {
	fetcher *fetcher.Fetcher

	// Configuration settings
	exemptAccounts []*reconciler.AccountCurrency
}

func NewBlockStorageHelper(
	fetcher *fetcher.Fetcher,
	exemptAccounts []*reconciler.AccountCurrency,
) *BlockStorageHelper {
	return &BlockStorageHelper{
		fetcher:        fetcher,
		exemptAccounts: exemptAccounts,
	}
}

func (h *BlockStorageHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) { // returns an error if lookupBalanceByBlock disabled
	return nil, errors.New("not implemented")
}

// SkipOperation returns a boolean indicating whether
// an operation should be processed. An operation will
// not be processed if it is considered unsuccessful
// or affects an exempt account.
func (h *BlockStorageHelper) SkipOperation(
	ctx context.Context,
	op *types.Operation,
) (bool, error) {
	successful, err := h.fetcher.Asserter.OperationSuccessful(op)
	if err != nil {
		// Should only occur if responses not validated
		return false, err
	}

	if !successful {
		return true, nil
	}

	if op.Account == nil {
		return true, nil
	}

	// Exempting account in BalanceChanges ensures that storage is not updated
	// and that the account is not reconciled.
	if h.accountExempt(ctx, op.Account, op.Amount.Currency) {
		log.Printf("Skipping exempt account %+v\n", op.Account)
		return true, nil
	}

	return false, nil
}

// accountExempt returns a boolean indicating if the provided
// account and currency are exempt from balance tracking and
// reconciliation.
func (h *BlockStorageHelper) accountExempt(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
) bool {
	return reconciler.ContainsAccountCurrency(
		h.exemptAccounts,
		&reconciler.AccountCurrency{
			Account:  account,
			Currency: currency,
		},
	)
}
