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
	"fmt"
	"log"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/processor"
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

	accountFile string
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
	checkCompleteCmd.Flags().StringVar(
		&accountFile,
		"interesting-accounts",
		"",
		`Absolute path to a file listing all accounts to check on each block. Look
at the examples directory for an example of how to structure this file.`,
	)
}

func runCheckCompleteCmd(cmd *cobra.Command, args []string) {
	// TODO: if no directory passed in, create a temporary one
	ctx, cancel := context.WithCancel(context.Background())

	exemptAccounts, err := loadAccounts(ExemptFile)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to load exempt accounts", err))
	}

	interestingAccounts, err := loadAccounts(accountFile)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to load interesting accounts", err))
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
		log.Fatal(fmt.Errorf("%w: unable to initialize asserter", err))
	}

	localStore, err := storage.NewBadgerStorage(ctx, DataDir)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to initialize data store", err))
	}

	logger := logger.NewLogger(
		DataDir,
		LogBlocks,
		LogTransactions,
		LogBalanceChanges,
		LogReconciliations,
	)

	blockStorageHelper := processor.NewBlockStorageHelper(fetcher, exemptAccounts)

	blockStorage := storage.NewBlockStorage(ctx, localStore, blockStorageHelper)
	if len(BootstrapBalances) > 0 {
		err = blockStorage.BootstrapBalances(
			ctx,
			BootstrapBalances,
			networkStatus.GenesisBlockIdentifier,
		)
		if err != nil {
			log.Fatal(fmt.Errorf("%w: unable to bootstrap balances", err))
		}
	}

	reconcilerHelper := &processor.ReconcilerHelper{}
	reconcilerHandler := &processor.ReconcilerHandler{}

	r := reconciler.NewReconciler(
		primaryNetwork,
		reconcilerHelper,
		reconcilerHandler,
		fetcher,
		AccountConcurrency,
		LookupBalanceByBlock,
		interestingAccounts,
	)

	syncHandler := processor.NewSyncHandler(
		blockStorage,
		logger,
		r,
		fetcher,
		exemptAccounts,
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	syncer := syncer.New(
		primaryNetwork,
		fetcher,
		syncHandler,
		cancel,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			StartIndex,
			EndIndex,
		)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
