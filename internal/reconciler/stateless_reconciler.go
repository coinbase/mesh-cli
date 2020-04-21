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

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"golang.org/x/sync/errgroup"
)

type StatelessReconciler struct {
	network                   *types.NetworkIdentifier
	fetcher                   *fetcher.Fetcher
	logger                    *logger.Logger
	accountConcurrency        uint64
	haltOnReconciliationError bool
	changeQueue               chan *storage.BalanceChange
}

func NewStateless(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency uint64,
	haltOnReconciliationError bool,
) Reconciler {
	return &StatelessReconciler{
		network:                   network,
		fetcher:                   fetcher,
		logger:                    logger,
		accountConcurrency:        accountConcurrency,
		haltOnReconciliationError: haltOnReconciliationError,
		changeQueue:               make(chan *storage.BalanceChange),
	}
}

func (r *StatelessReconciler) QueueChanges(
	ctx context.Context,
	balanceChanges []*storage.BalanceChange,
) error {
	// block until all checked for a block
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
