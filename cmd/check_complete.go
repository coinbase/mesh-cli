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

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/syncer"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkCompleteCmd = &cobra.Command{
		Use:   "check:complete",
		Short: "Run a full check of the correctness of a Rosetta server",
		Long: `Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off. If you want
to discard some number of blocks populate the --start flag with some block
index less than the last computed block index.`,
		Run: runCheckCompleteCmd,
	}

	// BootstrapBalances is a path to a file used to bootstrap
	// balances before starting syncing. Populating this value
	// after beginning syncing will return an error.
	BootstrapBalances string

	// LookupBalanceByBlock determines if balances are looked up
	// at the block where a balance change occurred instead of at the current
	// block. Blockchains that do not support historical balance lookup
	// should set this to false.
	LookupBalanceByBlock bool
)

func init() {
	checkCompleteCmd.Flags().StringVar(
		&BootstrapBalances,
		"bootstrap-balances",
		"",
		`Absolute path to a file used to bootstrap balances before starting syncing.
Populating this value after beginning syncing will return an error.`,
	)
	checkCompleteCmd.Flags().BoolVar(
		&LookupBalanceByBlock,
		"lookup-balance-by-block",
		true,
		`When set to true, balances are looked up at the block where a balance
change occurred instead of at the current block. Blockchains that do not support
historical balance lookup should set this to false.`,
	)
}

func runCheckCompleteCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())

	exemptAccounts, err := loadAccounts(ExemptFile)
	if err != nil {
		log.Fatalf("%w: unable to load exempt accounts", err)
	}

	fetcher := fetcher.New(
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
		fetcher.WithRetryElapsedTime(ExtendedRetryElapsedTime),
	)

	// TODO: sync and reconcile on subnetworks, if they exist.
	primaryNetwork, networkStatus, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatalf("%w: unable to initialize asserter", err)
	}

	localStore, err := storage.NewBadgerStorage(ctx, DataDir)
	if err != nil {
		log.Fatalf("%w: unable to initialize data store", err)
	}

	blockStorage := storage.NewBlockStorage(ctx, localStore)
	if len(BootstrapBalances) > 0 {
		err = blockStorage.BootstrapBalances(
			ctx,
			BootstrapBalances,
			networkStatus.GenesisBlockIdentifier,
		)
		if err != nil {
			log.Fatalf("%w: unable to bootstrap balances", err)
		}
	}

	logger := logger.NewLogger(
		DataDir,
		LogBlocks,
		LogTransactions,
		LogBalanceChanges,
		LogReconciliations,
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

	syncHandler := syncer.NewBaseHandler(
		logger,
		r,
		exemptAccounts,
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
