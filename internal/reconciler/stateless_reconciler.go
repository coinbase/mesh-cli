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
	"fmt"
	"math/big"

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
	acctQueue                 chan *changeAndBlock
}

func NewStateless(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency uint64,
	haltOnReconciliationError bool,
) *StatelessReconciler {
	return &StatelessReconciler{
		network:                   network,
		fetcher:                   fetcher,
		logger:                    logger,
		accountConcurrency:        accountConcurrency,
		haltOnReconciliationError: haltOnReconciliationError,
		acctQueue:                 make(chan *changeAndBlock),
	}
}

type changeAndBlock struct {
	change      *storage.BalanceChange
	parentBlock *types.BlockIdentifier
}

func (r *StatelessReconciler) QueueAccounts(
	ctx context.Context,
	parentBlock *types.BlockIdentifier,
	balanceChanges []*storage.BalanceChange,
) error {
	// block until all checked for a block
	for _, balanceChange := range balanceChanges {
		select {
		case r.acctQueue <- &changeAndBlock{
			change:      balanceChange,
			parentBlock: parentBlock,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (r *StatelessReconciler) balanceAtBlock(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*big.Int, error) {
	// Get balance before
	_, liveBalances, _, err := r.fetcher.AccountBalanceRetry(
		ctx,
		r.network,
		account,
		types.ConstructPartialBlockIdentifier(block),
	)
	if err != nil {
		return nil, err
	}

	liveAmount, err := extractAmount(liveBalances, currency)
	if err != nil {
		return nil, err
	}

	liveValue, ok := new(big.Int).SetString(liveAmount.Value, 10)
	if !ok {
		return nil, fmt.Errorf(
			"could not extract amount for %s",
			liveAmount.Value,
		)
	}

	return liveValue, nil
}

func (r *StatelessReconciler) reconcileChange(
	ctx context.Context,
	parentBlock *types.BlockIdentifier,
	change *storage.BalanceChange,
) error {
	// get balance before
	balanceBefore, err := r.balanceAtBlock(
		ctx,
		change.Account,
		change.Currency,
		parentBlock,
	)
	if err != nil {
		return err
	}

	// get balance after
	balanceAfter, err := r.balanceAtBlock(
		ctx,
		change.Account,
		change.Currency,
		change.Block,
	)
	if err != nil {
		return err
	}

	nodeDifference := new(big.Int).Sub(balanceAfter, balanceBefore)
	computedDifference, ok := new(big.Int).SetString(change.Difference, 10)
	if !ok {
		return fmt.Errorf(
			"could not extract amount for %s",
			change.Difference,
		)
	}
	difference := new(big.Int).Sub(computedDifference, nodeDifference).String()

	if difference != zeroString {
		err := r.logger.ReconcileFailureStream(
			ctx,
			activeReconciliation,
			change.Account,
			change.Currency,
			difference,
			"(computed-node)",
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
			Value:    balanceAfter.String(),
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
		case balanceChange := <-r.acctQueue:
			err := r.reconcileChange(
				ctx,
				balanceChange.parentBlock,
				balanceChange.change,
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
