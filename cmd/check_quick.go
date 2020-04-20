package cmd

import (
	"context"
	"log"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/syncer"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkQuickCmd = &cobra.Command{
		Use:   "check:quick",
		Short: "Check that blocks are mostly correct",
		Run:   runCheckQuickCmd,
	}
)

func init() {
	checkQuickCmd.Flags().Int64Var(
		&StartIndex,
		"start-index",
		-1,
		"start validation from some index",
	)
	checkQuickCmd.Flags().Int64Var(
		&EndIndex,
		"end-index",
		-1,
		"end validation at some index",
	)
}

func runCheckQuickCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())

	fetcher := fetcher.New(
		ctx,
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
	)

	// TODO: sync and reconcile on subnetworks, if they exist.
	primaryNetwork, _, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	logger := logger.NewLogger(
		DataDir,
		LogTransactions,
		LogBalances,
		LogReconciliation,
	)

	g, ctx := errgroup.WithContext(ctx)

	r := reconciler.NewStateless(
		primaryNetwork,
		fetcher,
		logger,
		AccountConcurrency,
		HaltOnReconciliationError,
	)

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	syncHandler := syncer.NewStatelessHandler(
		logger,
		r,
	)

	syncer := syncer.NewStateless(
		primaryNetwork,
		fetcher,
		syncHandler,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			cancel,
			StartIndex,
			EndIndex,
		)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
