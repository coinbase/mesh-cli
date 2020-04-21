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
	"github.com/coinbase/rosetta-validator/internal/storage"
	"github.com/coinbase/rosetta-validator/internal/syncer"

	"github.com/coinbase/rosetta-sdk-go/fetcher"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkCompleteCmd = &cobra.Command{
		Use:   "check:complete",
		Short: "Check the correctness of all blocks",
		Long: `Fetch each block and check that reconciles, no duplicates,
responses correct, etc. Handles re-orgs.
		`,
		Run: runCheckCompleteCmd,
	}
)

func init() {
	checkCompleteCmd.Flags().StringVar(
		&BootstrapBalances,
		"bootstrap-balances",
		"",
		"bootstrap balances from an initialization file (check examples directory for an example)",
	)
	checkCompleteCmd.Flags().BoolVar(
		&LookupBalanceByBlock,
		"lookup-balance-by-block",
		true,
		"perform reconciliation by looking up balances by block.",
	)
}

func runCheckCompleteCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())

	fetcher := fetcher.New(
		ctx,
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
	)

	// TODO: sync and reconcile on subnetworks, if they exist.
	primaryNetwork, networkStatus, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	localStore, err := storage.NewBadgerStorage(ctx, DataDir)
	if err != nil {
		log.Fatal(err)
	}

	blockStorage := storage.NewBlockStorage(ctx, localStore)
	if len(BootstrapBalances) > 0 {
		err = blockStorage.BootstrapBalances(
			ctx,
			BootstrapBalances,
			networkStatus.GenesisBlockIdentifier,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	logger := logger.NewLogger(
		DataDir,
		LogTransactions,
		LogBalances,
		LogReconciliation,
	)

	g, ctx := errgroup.WithContext(ctx)

	r := reconciler.NewStateful(
		primaryNetwork,
		blockStorage,
		fetcher,
		logger,
		AccountConcurrency,
		LookupBalanceByBlock,
		HaltOnReconciliationError,
	)

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	syncHandler := syncer.NewStatefulHandler(
		logger,
		r,
	)

	statefulSyncer := syncer.NewStateful(
		primaryNetwork,
		blockStorage,
		fetcher,
		syncHandler,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			cancel,
			statefulSyncer,
			StartIndex,
			EndIndex,
		)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
