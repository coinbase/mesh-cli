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
	"math/big"
	"math/rand"
	"reflect"
	"time"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"

	"github.com/davecgh/go-spew/spew"
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

	// accountBalanceMethod is used to determine if reconciliation
	// should be performed. If this method is not returned in
	// rosetta.Options.Methods, reconciliation is disabled.
	accountBalanceMethod = "/account/balance"

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

// Reconciler contains all logic to reconcile balances of
// rosetta.AccountIdentifiers returned in rosetta.Operations
// by a Rosetta Server.
type Reconciler struct {
	network            *rosetta.NetworkIdentifier
	storage            *storage.BlockStorage
	fetcher            *fetcher.Fetcher
	logger             *logger.Logger
	accountConcurrency int
	acctQueue          chan *storage.BalanceChange

	// highWaterMark is used to skip requests when
	// we are very far behind the live head.
	highWaterMark int64

	// seenAccts are stored for inactive account
	// reconciliation.
	seenAccts []*storage.BalanceChange
}

// New creates a new Reconciler.
func New(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	blockStorage *storage.BlockStorage,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency int,
) *Reconciler {
	return &Reconciler{
		network:            network,
		storage:            blockStorage,
		fetcher:            fetcher,
		logger:             logger,
		accountConcurrency: accountConcurrency,
		acctQueue:          make(chan *storage.BalanceChange, backlogThreshold),
		highWaterMark:      0,
		seenAccts:          make([]*storage.BalanceChange, 0),
	}
}

// containsAccountAndCurrency returns a boolean indicating if a
// BalanceChange slice already contains an Account and Currency combination.
func containsAccountAndCurrency(
	arr []*storage.BalanceChange,
	change *storage.BalanceChange,
) bool {
	for _, a := range arr {
		if reflect.DeepEqual(a.Account, change.Account) &&
			reflect.DeepEqual(a.Currency, change.Currency) {
			return true
		}
	}

	return false
}

// QueueAccounts adds an IndexAndAccount to the acctQueue
// for reconciliation.
func (r *Reconciler) QueueAccounts(
	ctx context.Context,
	blockIndex int64,
	balanceChanges []*storage.BalanceChange,
) {
	// If reconciliation is disabled,
	// we should just return.
	if r == nil {
		return
	}

	if blockIndex < r.highWaterMark {
		return
	}

	modifiedAccounts := make([]*storage.BalanceChange, 0)

	// Use a buffered channel so don't need to
	// spawn a goroutine to add accounts to channel.
	for _, balanceChange := range balanceChanges {
		// Remove duplicates
		if containsAccountAndCurrency(modifiedAccounts, balanceChange) {
			continue
		}
		modifiedAccounts = append(modifiedAccounts, balanceChange)

		select {
		case r.acctQueue <- balanceChange:
		default:
			log.Printf("skipping enqueue because backlog\n")
		}
	}
}

// CompareBalance checks to see if the computed balance of an account
// is equal to the live balance of an account. This function ensures
// balance is checked correctly in the case of orphaned blocks.
func (r *Reconciler) CompareBalance(
	ctx context.Context,
	accountAndCurrency *storage.BalanceChange,
	liveAmount *rosetta.Amount,
	liveBlock *rosetta.BlockIdentifier,
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
	amounts, balanceBlock, err := r.storage.GetBalance(ctx, txn, accountAndCurrency.Account)
	if err != nil {
		return zeroString, head.Index, err
	}

	if liveBlock.Index < balanceBlock.Index {
		return zeroString, head.Index, fmt.Errorf(
			"%w %+v updated at %d",
			ErrAccountUpdated,
			accountAndCurrency.Account,
			balanceBlock.Index,
		)
	}

	// Check balances are equal
	computedAmount, ok := amounts[storage.GetCurrencyKey(accountAndCurrency.Currency)]
	if !ok {
		return zeroString, head.Index, fmt.Errorf(
			"currency %+v not found",
			*accountAndCurrency.Currency,
		)
	}

	if computedAmount.Value != liveAmount.Value {
		computed, ok := new(big.Int).SetString(computedAmount.Value, 10)
		if !ok {
			return zeroString, head.Index, fmt.Errorf(
				"could not extract amount for %s",
				computedAmount.Value,
			)
		}
		live, ok := new(big.Int).SetString(liveAmount.Value, 10)
		if !ok {
			return zeroString, head.Index, fmt.Errorf(
				"could not extract amount for %s",
				liveAmount.Value,
			)
		}

		return new(big.Int).Sub(computed, live).String(), head.Index, nil
	}

	return zeroString, head.Index, nil
}

// extractAmount returns the rosetta.Amount from a slice of rosetta.Balance
// pertaining to an AccountAndCurrency.
func extractAmount(
	balances []*rosetta.Balance,
	accountAndCurrency *storage.BalanceChange,
) (*rosetta.Amount, error) {
	for _, b := range balances {
		if !reflect.DeepEqual(b.AccountIdentifier, accountAndCurrency.Account) {
			continue
		}

		for _, amount := range b.Amounts {
			if !reflect.DeepEqual(amount.Currency, accountAndCurrency.Currency) {
				continue
			}

			return amount, nil
		}
	}

	return nil, fmt.Errorf("could not extract amount for %+v", accountAndCurrency)
}

// ShouldReconcile returns a boolean indicating whether reconciliation
// should be attempted based on what methods the Rosetta Server implements.
func ShouldReconcile(networkStatus *rosetta.NetworkStatusResponse) bool {
	for _, method := range networkStatus.Options.Methods {
		if method == accountBalanceMethod {
			return true
		}
	}

	return false
}

// accountReconciliation returns an error if the provided
// AccountAndCurrency's live balance cannot be reconciled
// with the computed balance.
func (r *Reconciler) accountReconciliation(
	ctx context.Context,
	acct *storage.BalanceChange,
	inactive bool,
) error {
	start := time.Now()
	liveBlock, liveBalances, err := r.fetcher.AccountBalanceRetry(
		ctx,
		r.network,
		acct.Account,
		fetcher.DefaultElapsedTime,
		fetcher.DefaultRetries,
	)
	if err != nil {
		return err
	}

	err = r.logger.AccountLatency(ctx, acct.Account, time.Since(start).Seconds(), len(liveBalances))
	if err != nil {
		return err
	}

	liveAmount, err := extractAmount(liveBalances, acct)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		difference, headIndex, err := r.CompareBalance(
			ctx,
			acct,
			liveAmount,
			liveBlock,
		)
		if err != nil {
			if errors.Is(err, ErrHeadBlockBehindLive) {
				diff := liveBlock.Index - headIndex
				if diff < waitToCheckDiff {
					time.Sleep(waitToCheckDiffSleep)
					continue
				}

				// Don't wait if we are very far behind
				log.Printf(
					"Skipping reconciliation for %s: %d blocks behind\n",
					simpleAccountAndCurrency(acct),
					diff,
				)
				r.highWaterMark = liveBlock.Index
				break
			} else if errors.Is(err, ErrBlockGone) {
				// Either the block has not been processed in a re-org yet
				// or the block was orphaned
				break
			} else if errors.Is(err, ErrAccountUpdated) {
				break // account will already be re-checked
			} else {
				return err
			}
		}

		reconciliationType := activeReconciliation
		if inactive {
			reconciliationType = inactiveReconciliation
		}

		if difference != zeroString {
			return fmt.Errorf(
				"\n%s balance mismatch\naccount: %+v\ncurrency: %+v\nblock: %+v\nbalance difference(computed-live):%s",
				reconciliationType,
				spew.Sdump(acct.Account),
				spew.Sdump(acct.Currency),
				spew.Sdump(liveBlock),
				difference,
			)
		}

		if !inactive && !containsAccountAndCurrency(r.seenAccts, acct) {
			r.seenAccts = append(r.seenAccts, acct)
		}

		log.Printf(
			"Reconciled %s %s at %d\n",
			reconciliationType,
			simpleAccountAndCurrency(acct),
			liveBlock.Index,
		)
		break
	}

	return nil
}

// simpleAccountAndCurrency returns a string that is a simple
// representation of an AccountAndCurrency struct.
func simpleAccountAndCurrency(acct *storage.BalanceChange) string {
	acctString := acct.Account.Address
	if acct.Account.SubAccount != nil {
		acctString += acct.Account.SubAccount.SubAccount
	}

	acctString += acct.Currency.Symbol

	return acctString
}

// reconcileActiveAccounts selects an account
// from the reconciler account queue and
// reconciles the balance. This is useful
// for detecting if balance changes in operations
// were correct.
func (r *Reconciler) reconcileActiveAccounts(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case balanceChange := <-r.acctQueue:
			if balanceChange.Block.Index < r.highWaterMark {
				continue
			}

			err := r.accountReconciliation(
				ctx,
				balanceChange,
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
func (r *Reconciler) reconcileInactiveAccounts(
	ctx context.Context,
) error {
	randSource := rand.NewSource(time.Now().UnixNano())
	randGenerator := rand.New(randSource)
	for ctx.Err() == nil {
		if len(r.seenAccts) > 0 {
			randAcct := r.seenAccts[randGenerator.Intn(len(r.seenAccts))]

			err := r.accountReconciliation(ctx, randAcct, true)
			if err != nil {
				return err
			}
		} else {
			time.Sleep(inactiveReconciliationSleep)
		}
	}

	return nil
}

// Reconcile starts the active and inactive reconciler goroutines.
// If either set of goroutines errors, the function will return an error.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for j := 0; j < r.accountConcurrency; j++ {
		g.Go(func() error {
			return r.reconcileActiveAccounts(ctx)
		})
	}

	g.Go(func() error {
		return r.reconcileInactiveAccounts(ctx)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
