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
	"fmt"
	"log"

	"github.com/coinbase/rosetta-cli/internal/reconciler"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// BlockStorageHelper implements the storage.Helper
// interface.
type BlockStorageHelper struct {
	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher

	// Configuration settings
	lookupBalanceByBlock bool
	exemptAccounts       []*reconciler.AccountCurrency
}

// NewBlockStorageHelper returns a new BlockStorageHelper.
func NewBlockStorageHelper(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	lookupBalanceByBlock bool,
	exemptAccounts []*reconciler.AccountCurrency,
) *BlockStorageHelper {
	return &BlockStorageHelper{
		network:              network,
		fetcher:              fetcher,
		lookupBalanceByBlock: lookupBalanceByBlock,
		exemptAccounts:       exemptAccounts,
	}
}

// AccountBalance attempts to fetch the balance
// for a missing account in storage. This is necessary
// for running the "check" command at an arbitrary height
// instead of syncing from genesis.
func (h *BlockStorageHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	if !h.lookupBalanceByBlock {
		return &types.Amount{
			Value:    "0",
			Currency: currency,
		}, nil
	}

	// In the case that we are syncing from arbitrary height,
	// we may need to recover the balance of an account to
	// perform validations.
	_, value, err := reconciler.GetCurrencyBalance(
		ctx,
		h.fetcher,
		h.network,
		account,
		currency,
		types.ConstructPartialBlockIdentifier(block),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get currency balance in storage helper", err)
	}

	return &types.Amount{
		Value:    value,
		Currency: currency,
	}, nil
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
	if h.accountExempt(op.Account, op.Amount.Currency) {
		log.Printf("Skipping exempt account %+v\n", op.Account)
		return true, nil
	}

	return false, nil
}

// accountExempt returns a boolean indicating if the provided
// account and currency are exempt from balance tracking and
// reconciliation.
func (h *BlockStorageHelper) accountExempt(
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
