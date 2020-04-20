package reconciler

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/sync/errgroup"
)

type StatelessReconciler struct {
	network            *types.NetworkIdentifier
	fetcher            *fetcher.Fetcher
	logger             *logger.Logger
	accountConcurrency uint64
	acctQueue          chan *changeAndBlock
}

func NewStateless(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	accountConcurrency uint64,
) *StatelessReconciler {
	return &StatelessReconciler{
		network:            network,
		fetcher:            fetcher,
		logger:             logger,
		accountConcurrency: accountConcurrency,
		acctQueue:          make(chan *changeAndBlock),
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

	difference := new(big.Int).Sub(balanceAfter, balanceBefore).String()

	if difference != change.Difference {
		return fmt.Errorf(
			"\nbalance mismatch\naccount: %+v\ncurrency: %+v\nblock: %+v\nbalance difference(computed-live):%s",
			spew.Sdump(change.Account),
			spew.Sdump(change.Currency),
			spew.Sdump(change.Block),
			difference,
		)
	}

	log.Printf(
		"Reconciled %s at %d\n",
		simpleAccountAndCurrency(change),
		change.Block.Index,
	)

	return r.logger.ReconcileStream(
		ctx,
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
