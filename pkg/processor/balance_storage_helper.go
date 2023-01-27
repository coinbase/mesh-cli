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
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ modules.BalanceStorageHelper = (*BalanceStorageHelper)(nil)

// BalanceStorageHelper implements the storage.Helper
// interface.
type BalanceStorageHelper struct {
	network        *types.NetworkIdentifier
	fetcher        *fetcher.Fetcher
	counterStorage *modules.CounterStorage

	// Configuration settings
	lookupBalanceByBlock bool
	exemptAccounts       map[string]struct{}
	balanceExemptions    []*types.BalanceExemption
	initialFetchDisabled bool

	// Interesting-only Parsing
	interestingOnly      bool
	interestingAddresses map[string]struct{}
}

// NewBalanceStorageHelper returns a new BalanceStorageHelper.
func NewBalanceStorageHelper(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	counterStorage *modules.CounterStorage,
	lookupBalanceByBlock bool,
	exemptAccounts []*types.AccountCurrency,
	interestingOnly bool,
	balanceExemptions []*types.BalanceExemption,
	initialFetchDisabled bool,
) *BalanceStorageHelper {
	exemptMap := map[string]struct{}{}

	// Pre-process exemptAccounts on initialization
	// to provide fast lookup while syncing.
	for _, account := range exemptAccounts {
		// if users do not specify Currency, we add the address
		// by this, all the Currencies in this address will be skipped
		if account.Currency == nil {
			exemptMap[account.Account.Address] = struct{}{}
		} else {
			exemptMap[types.Hash(account)] = struct{}{}
		}
	}

	return &BalanceStorageHelper{
		network:              network,
		fetcher:              fetcher,
		counterStorage:       counterStorage,
		lookupBalanceByBlock: lookupBalanceByBlock,
		exemptAccounts:       exemptMap,
		interestingAddresses: map[string]struct{}{},
		interestingOnly:      interestingOnly,
		balanceExemptions:    balanceExemptions,
		initialFetchDisabled: initialFetchDisabled,
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
	lookupBlock *types.BlockIdentifier,
) (*types.Amount, error) {
	if !h.lookupBalanceByBlock || h.initialFetchDisabled {
		return &types.Amount{
			Value:    "0",
			Currency: currency,
		}, nil
	}

	// In the case that we are syncing from arbitrary height,
	// we may need to recover the balance of an account to
	// perform validations.
	amount, block, err := utils.CurrencyBalance(
		ctx,
		h.network,
		h.fetcher,
		account,
		currency,
		lookupBlock.Index,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get balance of currency %s for account %s: %w", types.PrintStruct(currency), types.PrintStruct(account), err)
	}

	// If the returned balance block does not match the intended
	// block a re-org could've occurred.
	if types.Hash(lookupBlock) != types.Hash(block) {
		return nil, syncer.ErrOrphanHead
	}

	return &types.Amount{
		Value:    amount.Value,
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
		// if exemptAccounts have the Account address means all the
		// currencies in this Account address need to be skipped
		_, existsAddress := h.exemptAccounts[op.Account.Address]
		if existsAddress {
			return existsAddress
		}

		thisAcct := types.Hash(&types.AccountCurrency{
			Account:  op.Account,
			Currency: op.Amount.Currency,
		})

		_, exists := h.exemptAccounts[thisAcct]
		return exists
	}
}

// BalanceExemptions returns a list of *types.BalanceExemption.
func (h *BalanceStorageHelper) BalanceExemptions() []*types.BalanceExemption {
	return h.balanceExemptions
}

// AccountsReconciled returns the total accounts reconciled by count.
func (h *BalanceStorageHelper) AccountsReconciled(
	ctx context.Context,
	dbTx database.Transaction,
) (*big.Int, error) {
	return h.counterStorage.GetTransactional(ctx, dbTx, modules.ReconciledAccounts)
}

// AccountsSeen returns the total accounts seen by count.
func (h *BalanceStorageHelper) AccountsSeen(
	ctx context.Context,
	dbTx database.Transaction,
) (*big.Int, error) {
	return h.counterStorage.GetTransactional(ctx, dbTx, modules.SeenAccounts)
}
