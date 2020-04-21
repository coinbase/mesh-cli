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
		Short: "Run a simple check of the correctness of a Rosetta server",
		Long: `Check all server responses are properly constructed and that
computed balance changes are equal to balance changes reported by the
node. To use check:quick, your server must implement the balance lookup
by block. Unlike check:complete, which requires syncing all blocks up
to the blocks you want to check, check:quick allows you to validate
an arbitrary range of blocks (even if earlier blocks weren't synced).

It is important to note that check:quick does not support re-orgs and it
does not check for duplicate blocks and transactions. For these features,
please use check:complete.`,
		Run: runCheckQuickCmd,
	}
)

func runCheckQuickCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())

	fetcher := fetcher.New(
		ctx,
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
	)

	primaryNetwork, _, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	logger := logger.NewLogger(
		DataDir,
		LogBlocks,
		LogTransactions,
		LogBalanceChanges,
		LogReconciliations,
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

	syncHandler := syncer.NewBaseHandler(
		logger,
		r,
	)

	statelessSyncer := syncer.NewStateless(
		primaryNetwork,
		fetcher,
		syncHandler,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			cancel,
			statelessSyncer,
			StartIndex,
			EndIndex,
		)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
