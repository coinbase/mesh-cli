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

package reconciler

import (
	"context"
	"errors"
	"reflect"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"golang.org/x/sync/errgroup"
)

// StatelessReconciler compares computed balances with
// the node balance without using any sort of persistent
// state.	If it is not possible to lookup a balance by block,
// you must use the StatefulReconciler. If you want to perform
// inactive reconciliation (check for balance changes that
// occurred that were not in blocks), you must also use the
// StatefulReconciler. Lastly, the StatelessReconciler does
// not support re-orgs.
type StatelessReconciler struct {
	network                   *types.NetworkIdentifier
	fetcher                   *fetcher.Fetcher
	logger                    *logger.Logger
	accountConcurrency        uint64
	haltOnReconciliationError bool
	interestingAccounts       []*AccountCurrency
	changeQueue               chan *storage.BalanceChange
}

// NewStateless returns a new StatelessReconciler.
func NewStateless(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency uint64,
	haltOnReconciliationError bool,
	interestingAccounts []*AccountCurrency,
) *StatelessReconciler {
	return &StatelessReconciler{
		network:                   network,
		fetcher:                   fetcher,
		logger:                    logger,
		accountConcurrency:        accountConcurrency,
		haltOnReconciliationError: haltOnReconciliationError,
		interestingAccounts:       interestingAccounts,
		changeQueue:               make(chan *storage.BalanceChange),
	}
}

// QueueChanges enqueues a slice of *storage.BalanceChanges
// for reconciliation.
func (r *StatelessReconciler) QueueChanges(
	ctx context.Context,
	block *types.BlockIdentifier,
	balanceChanges []*storage.BalanceChange,
) error {
	// Ensure all interestingAccounts are checked
	// TODO: refactor to automatically trigger once an inactive reconciliation error
	// is discovered
	for _, account := range r.interestingAccounts {
		skipAccount := false
		// Look through balance changes for account + currency
		for _, change := range balanceChanges {
			if reflect.DeepEqual(change.Account, account.Account) && reflect.DeepEqual(change.Currency, account.Currency) {
				skipAccount = true
				break
			}
		}

		// Account changed on this block
		if skipAccount {
			continue
		}

		// If account + currency not found, add with difference 0
		balanceChanges = append(balanceChanges, &storage.BalanceChange{
			Account:    account.Account,
			Currency:   account.Currency,
			Difference: "0",
			Block:      block,
		})
	}

	// Block until all checked for a block
	for _, change := range balanceChanges {
		select {
		case r.changeQueue <- change:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (r *StatelessReconciler) balanceAtIndex(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (string, error) {
	_, value, err := GetCurrencyBalance(
		ctx,
		r.fetcher,
		r.network,
		account,
		currency,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)

	return value, err
}

func (r *StatelessReconciler) reconcileChange(
	ctx context.Context,
	change *storage.BalanceChange,
) error {
	// Get balance at block before change
	balanceBefore, err := r.balanceAtIndex(
		ctx,
		change.Account,
		change.Currency,
		change.Block.Index-1,
	)
	if err != nil {
		return err
	}

	// Get balance at block with change
	balanceAfter, err := r.balanceAtIndex(
		ctx,
		change.Account,
		change.Currency,
		change.Block.Index,
	)
	if err != nil {
		return err
	}

	// Get difference between node change and computed change
	nodeDifference, err := storage.SubtractStringValues(balanceAfter, balanceBefore)
	if err != nil {
		return err
	}

	difference, err := storage.SubtractStringValues(change.Difference, nodeDifference)
	if err != nil {
		return err
	}

	if difference != zeroString {
		err := r.logger.ReconcileFailureStream(
			ctx,
			activeReconciliation,
			change.Account,
			change.Currency,
			difference,
			change.Block,
		)
		if err != nil {
			return err
		}

		if r.haltOnReconciliationError {
			return errors.New("reconciliation error")
		}

		return nil
	}

	return r.logger.ReconcileSuccessStream(
		ctx,
		activeReconciliation,
		change.Account,
		&types.Amount{
			Value:    balanceAfter,
			Currency: change.Currency,
		},
		change.Block,
	)
}

func (r *StatelessReconciler) reconcileAccounts(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case change := <-r.changeQueue:
			err := r.reconcileChange(
				ctx,
				change,
			)
			if err != nil {
				return err
			}
		}
	}
}

// Reconcile starts the active StatelessReconciler goroutines.
// If any goroutine errors, the function will return an error.
func (r *StatelessReconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for j := uint64(0); j < r.accountConcurrency; j++ {
		g.Go(func() error {
			return r.reconcileAccounts(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
