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
	"log"
	"math/rand"
	"time"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"

	"golang.org/x/sync/errgroup"
)

const (
	// backlogThreshold is the limit of account lookups
	// that can be enqueued to reconcile before new
	// requests are dropped.
	// TODO: Make configurable
	backlogThreshold = 1000

	// waitToCheckDiff is the syncing difference (live-head)
	// to retry instead of exiting. In other words, if the
	// processed head is behind the live head by <
	// waitToCheckDiff we should try again after sleeping.
	// TODO: Make configurable
	waitToCheckDiff = 10

	// waitToCheckDiffSleep is the amount of time to wait
	// to check a balance difference if the syncer is within
	// waitToCheckDiff from the block a balance was queried at.
	waitToCheckDiffSleep = 5 * time.Second

	// activeReconciliation is included in the reconciliation
	// error message if reconciliation failed during active
	// reconciliation.
	activeReconciliation = "ACTIVE"

	// inactiveReconciliation is included in the reconciliation
	// error message if reconciliation failed during inactive
	// reconciliation.
	inactiveReconciliation = "INACTIVE"

	// zeroString is a string of value 0.
	zeroString = "0"

	// inactiveReconciliationSleep is used as the time.Duration
	// to sleep when there are no seen accounts to reconcile.
	inactiveReconciliationSleep = 5 * time.Second
)

var (
	// ErrHeadBlockBehindLive is returned when the processed
	// head is behind the live head. Sometimes, it is
	// preferrable to sleep and wait to catch up when
	// we are close to the live head (waitToCheckDiff).
	ErrHeadBlockBehindLive = errors.New("head block behind")

	// ErrAccountUpdated is returned when the
	// account was updated at a height later than
	// the live height (when the account balance was fetched).
	ErrAccountUpdated = errors.New("account updated")

	// ErrBlockGone is returned when the processed block
	// head is greater than the live head but the block
	// does not exist in the store. This likely means
	// that the block was orphaned.
	ErrBlockGone = errors.New("block gone")
)

// StatefulReconciler contains all logic to reconcile balances of
// types.AccountIdentifiers returned in types.Operations
// by a Rosetta Server.
type StatefulReconciler struct {
	network                   *types.NetworkIdentifier
	storage                   *storage.BlockStorage
	fetcher                   *fetcher.Fetcher
	logger                    *logger.Logger
	accountConcurrency        uint64
	lookupBalanceByBlock      bool
	haltOnReconciliationError bool
	changeQueue               chan *storage.BalanceChange

	// highWaterMark is used to skip requests when
	// we are very far behind the live head.
	highWaterMark int64

	// seenAccts are stored for inactive account
	// reconciliation.
	seenAccts []*AccountCurrency
}

// NewStateful creates a new StatefulReconciler.
func NewStateful(
	network *types.NetworkIdentifier,
	blockStorage *storage.BlockStorage,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency uint64,
	lookupBalanceByBlock bool,
	haltOnReconciliationError bool,
) *StatefulReconciler {
	return &StatefulReconciler{
		network:                   network,
		storage:                   blockStorage,
		fetcher:                   fetcher,
		logger:                    logger,
		accountConcurrency:        accountConcurrency,
		lookupBalanceByBlock:      lookupBalanceByBlock,
		haltOnReconciliationError: haltOnReconciliationError,
		changeQueue:               make(chan *storage.BalanceChange, backlogThreshold),
		highWaterMark:             -1,
		seenAccts:                 make([]*AccountCurrency, 0),
	}
}

// QueueChanges enqueues a slice of *storage.BalanceChanges
// for reconciliation.
func (r *StatefulReconciler) QueueChanges(
	ctx context.Context,
	block *types.BlockIdentifier,
	balanceChanges []*storage.BalanceChange,
) error {
	// All changes will have the same block. Return
	// if we are too far behind to start reconciling.
	if block.Index < r.highWaterMark {
		return nil
	}

	// Use a buffered channel so don't need to
	// spawn a goroutine to add accounts to channel.
	for _, change := range balanceChanges {
		select {
		case r.changeQueue <- change:
		default:
			log.Printf("skipping enqueue because backlog\n")
		}
	}

	return nil
}

// CompareBalance checks to see if the computed balance of an account
// is equal to the live balance of an account. This function ensures
// balance is checked correctly in the case of orphaned blocks.
func (r *StatefulReconciler) CompareBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	amount string,
	liveBlock *types.BlockIdentifier,
) (string, int64, error) {
	txn := r.storage.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)

	// Head block should be set before we CompareBalance
	head, err := r.storage.GetHeadBlockIdentifier(ctx, txn)
	if err != nil {
		return zeroString, 0, err
	}

	// Check if live block is < head (or wait)
	if liveBlock.Index > head.Index {
		return zeroString, head.Index, fmt.Errorf(
			"%w live block %d > head block %d",
			ErrHeadBlockBehindLive,
			liveBlock.Index,
			head.Index,
		)
	}

	// Check if live block is in store (ensure not reorged)
	_, err = r.storage.GetBlock(ctx, txn, liveBlock)
	if err != nil {
		return zeroString, head.Index, fmt.Errorf(
			"%w %+v",
			ErrBlockGone,
			liveBlock,
		)
	}

	// Check if live block < computed head
	amounts, balanceBlock, err := r.storage.GetBalance(ctx, txn, account)
	if err != nil {
		return zeroString, head.Index, err
	}

	if liveBlock.Index < balanceBlock.Index {
		return zeroString, head.Index, fmt.Errorf(
			"%w %+v updated at %d",
			ErrAccountUpdated,
			account,
			balanceBlock.Index,
		)
	}

	// Check balances are equal
	computedAmount, ok := amounts[storage.GetCurrencyKey(currency)]
	if !ok {
		return "", head.Index, fmt.Errorf(
			"currency %+v not found",
			*currency,
		)
	}

	difference, err := storage.SubtractStringValues(computedAmount.Value, amount)
	if err != nil {
		return "", -1, err
	}

	if difference != zeroString {
		return difference, head.Index, nil
	}

	return zeroString, head.Index, nil
}

// getAccountBalance returns the balance for an account
// at either the current block (if lookupBalanceByBlock is
// disabled) or at some historical block.
func (r *StatefulReconciler) bestBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, string, error) {
	if !r.lookupBalanceByBlock {
		// Use the current balance to reconcile balances when lookupBalanceByBlock
		// is disabled. This could be the case when a rosetta server does not
		// support historical balance lookups.
		block = nil
	}
	return GetCurrencyBalance(
		ctx,
		r.fetcher,
		r.network,
		account,
		currency,
		block,
	)
}

// accountReconciliation returns an error if the provided
// AccountAndCurrency's live balance cannot be reconciled
// with the computed balance.
func (r *StatefulReconciler) accountReconciliation(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	liveAmount string,
	liveBlock *types.BlockIdentifier,
	inactive bool,
) error {
	accountCurrency := &AccountCurrency{
		Account:  account,
		Currency: currency,
	}
	for ctx.Err() == nil {
		difference, headIndex, err := r.CompareBalance(
			ctx,
			account,
			currency,
			liveAmount,
			liveBlock,
		)
		if err != nil {
			if errors.Is(err, ErrHeadBlockBehindLive) {
				// This error will only occur when lookupBalanceByBlock
				// is disabled and the syncer is behind the current block of
				// the node. This error should never occur when
				// lookupBalanceByBlock is enabled.
				diff := liveBlock.Index - headIndex
				if diff < waitToCheckDiff {
					time.Sleep(waitToCheckDiffSleep)
					continue
				}

				// Don't wait to check if we are very far behind
				log.Printf(
					"Skipping reconciliation for %s: %d blocks behind\n",
					simpleAccountCurrency(accountCurrency),
					diff,
				)

				// Set a highWaterMark to not accept any new
				// reconciliation requests unless they happened
				// after this new highWaterMark.
				r.highWaterMark = liveBlock.Index
				break
			}

			if errors.Is(err, ErrBlockGone) {
				// Either the block has not been processed in a re-org yet
				// or the block was orphaned
				break
			}

			if errors.Is(err, ErrAccountUpdated) {
				// If account was updated, it must be
				// enqueued again
				break
			}

			return err
		}

		reconciliationType := activeReconciliation
		if inactive {
			reconciliationType = inactiveReconciliation
		}

		if difference != zeroString {
			err := r.logger.ReconcileFailureStream(
				ctx,
				reconciliationType,
				accountCurrency.Account,
				accountCurrency.Currency,
				difference,
				liveBlock,
			)

			if err != nil {
				return err
			}

			if r.haltOnReconciliationError {
				return errors.New("reconciliation error")
			}

			return nil
		}

		if !inactive && !ContainsAccountCurrency(r.seenAccts, accountCurrency) {
			r.seenAccts = append(r.seenAccts, accountCurrency)
		}

		return r.logger.ReconcileSuccessStream(
			ctx,
			reconciliationType,
			accountCurrency.Account,
			&types.Amount{
				Value:    liveAmount,
				Currency: currency,
			},
			liveBlock,
		)
	}

	return nil
}

// simpleAccountCurrency returns a string that is a simple
// representation of an AccountCurrency struct.
func simpleAccountCurrency(
	accountCurrency *AccountCurrency,
) string {
	acctString := accountCurrency.Account.Address
	if accountCurrency.Account.SubAccount != nil {
		acctString += accountCurrency.Account.SubAccount.Address
	}

	acctString += accountCurrency.Currency.Symbol

	return acctString
}

// reconcileActiveAccounts selects an account
// from the StatefulReconciler account queue and
// reconciles the balance. This is useful
// for detecting if balance changes in operations
// were correct.
func (r *StatefulReconciler) reconcileActiveAccounts(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case balanceChange := <-r.changeQueue:
			if balanceChange.Block.Index < r.highWaterMark {
				continue
			}

			block, value, err := r.bestBalance(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				types.ConstructPartialBlockIdentifier(balanceChange.Block),
			)
			if err != nil {
				return err
			}

			err = r.accountReconciliation(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				value,
				block,
				false,
			)
			if err != nil {
				return err
			}
		}
	}
}

// reconcileInactiveAccounts selects a random account
// from all previously seen accounts and reconciles
// the balance. This is useful for detecting balance
// changes that were not returned in operations.
func (r *StatefulReconciler) reconcileInactiveAccounts(
	ctx context.Context,
) error {
	randSource := rand.NewSource(time.Now().UnixNano())
	randGenerator := rand.New(randSource)
	for ctx.Err() == nil {
		if len(r.seenAccts) > 0 {
			randAcct := r.seenAccts[randGenerator.Intn(len(r.seenAccts))]

			txn := r.storage.NewDatabaseTransaction(ctx, false)
			head, err := r.storage.GetHeadBlockIdentifier(ctx, txn)
			if err != nil {
				return err
			}
			txn.Discard(ctx)

			block, amount, err := r.bestBalance(
				ctx,
				randAcct.Account,
				randAcct.Currency,
				types.ConstructPartialBlockIdentifier(head),
			)
			if err != nil {
				return err
			}

			err = r.accountReconciliation(
				ctx,
				randAcct.Account,
				randAcct.Currency,
				amount,
				block,
				true,
			)
			if err != nil {
				return err
			}
		} else {
			time.Sleep(inactiveReconciliationSleep)
		}
	}

	return nil
}

// Reconcile starts the active and inactive StatefulReconciler goroutines.
// If any goroutine errors, the function will return an error.
func (r *StatefulReconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for j := uint64(0); j < r.accountConcurrency/2; j++ {
		g.Go(func() error {
			return r.reconcileActiveAccounts(ctx)
		})

		g.Go(func() error {
			return r.reconcileInactiveAccounts(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
