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

	"github.com/slowboat0/rosetta-cli/pkg/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ storage.BalanceStorageHelper = (*BalanceStorageHelper)(nil)

// BalanceStorageHelper implements the storage.Helper
// interface.
type BalanceStorageHelper struct {
	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher

	// Configuration settings
	lookupBalanceByBlock bool
	exemptAccounts       map[string]struct{}

	// Interesting-only Parsing
	interestingOnly      bool
	interestingAddresses map[string]struct{}
}

// NewBalanceStorageHelper returns a new BalanceStorageHelper.
func NewBalanceStorageHelper(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	lookupBalanceByBlock bool,
	exemptAccounts []*reconciler.AccountCurrency,
	interestingOnly bool,
) *BalanceStorageHelper {
	exemptMap := map[string]struct{}{}

	// Pre-process exemptAccounts on initialization
	// to provide fast lookup while syncing.
	for _, account := range exemptAccounts {
		exemptMap[types.Hash(account)] = struct{}{}
	}

	return &BalanceStorageHelper{
		network:              network,
		fetcher:              fetcher,
		lookupBalanceByBlock: lookupBalanceByBlock,
		exemptAccounts:       exemptMap,
		interestingAddresses: map[string]struct{}{},
		interestingOnly:      interestingOnly,
	}
}

// AccountBalance attempts to fetch the balance
// for a missing account in storage. This is necessary
// for running the "check" command at an arbitrary height
// instead of syncing from genesis.
func (h *BalanceStorageHelper) AccountBalance(
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

// Asserter returns a *asserter.Asserter.
func (h *BalanceStorageHelper) Asserter() *asserter.Asserter {
	return h.fetcher.Asserter
}

// AddInterestingAddress adds an address to track the balance of.
// This is often done after generating an account.
func (h *BalanceStorageHelper) AddInterestingAddress(address string) {
	h.interestingAddresses[address] = struct{}{}
}

// ExemptFunc returns a parser.ExemptOperation.
func (h *BalanceStorageHelper) ExemptFunc() parser.ExemptOperation {
	return func(op *types.Operation) bool {
		if h.interestingOnly {
			if _, exists := h.interestingAddresses[op.Account.Address]; !exists {
				return true
			}
		}

		thisAcct := types.Hash(&reconciler.AccountCurrency{
			Account:  op.Account,
			Currency: op.Amount.Currency,
		})

		_, exists := h.exemptAccounts[thisAcct]
		return exists
	}
}
