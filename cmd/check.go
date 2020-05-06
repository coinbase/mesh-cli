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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"time"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/processor"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/syncer"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	// ExtendedRetryElapsedTime is used to override the default fetcher
	// retry elapsed time. In practice, extending the retry elapsed time
	// has prevented retry exhaustion errors when many goroutines are
	// used to fetch data from the Rosetta server.
	//
	// TODO: make configurable
	ExtendedRetryElapsedTime = 5 * time.Minute
)

var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Run a full check of the correctness of a Rosetta server",
		Long: `Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off. If you want
to discard some number of blocks populate the --start flag with some block
index less than the last computed block index.`,
		Run: runCheckCmd,
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

	// DataDir is a folder used to store logs
	// and any data used to perform validation.
	DataDir string

	// StartIndex is the block index to start syncing.
	StartIndex int64

	// EndIndex is the block index to stop syncing.
	EndIndex int64

	// BlockConcurrency is the concurrency to use
	// while fetching blocks.
	BlockConcurrency uint64

	// TransactionConcurrency is the concurrency to use
	// while fetching transactions (if required).
	TransactionConcurrency uint64

	// AccountConcurrency is the concurrency to use
	// while fetching accounts during reconciliation.
	AccountConcurrency uint64

	// LogBlocks determines if blocks are
	// logged.
	LogBlocks bool

	// LogTransactions determines if transactions are
	// logged.
	LogTransactions bool

	// LogBalanceChanges determines if balance changes are
	// logged.
	LogBalanceChanges bool

	// LogReconciliations determines if reconciliations are
	// logged.
	LogReconciliations bool

	// HaltOnReconciliationError determines if processing
	// should stop when encountering a reconciliation error.
	// It can be beneficial to collect all reconciliation errors
	// during development.
	HaltOnReconciliationError bool

	// ExemptFile is an absolute path to a file listing all accounts
	// to exempt from balance tracking and reconciliation.
	ExemptFile string

	// InterestingFile is an absolute path to a file listing all accounts
	// to actively reconcile on each block (if there are no operations
	// present for the account, the reconciler asserts a balance change of 0).
	InterestingFile string
)

func loadAccounts(filePath string) ([]*reconciler.AccountCurrency, error) {
	if len(filePath) == 0 {
		return []*reconciler.AccountCurrency{}, nil
	}

	accountsRaw, err := ioutil.ReadFile(path.Clean(filePath))
	if err != nil {
		return nil, err
	}

	accounts := []*reconciler.AccountCurrency{}
	if err := json.Unmarshal(accountsRaw, &accounts); err != nil {
		return nil, err
	}

	log.Printf("Found %d accounts at %s: %s\n", len(accounts), filePath, types.PrettyPrintStruct(accounts))

	return accounts, nil
}

func init() {
	checkCmd.Flags().StringVar(
		&DataDir,
		"data-dir",
		"",
		"folder used to store logs and any data used to perform validation",
	)
	checkCmd.Flags().Int64Var(
		&StartIndex,
		"start",
		-1,
		"block index to start syncing",
	)
	checkCmd.Flags().Int64Var(
		&EndIndex,
		"end",
		-1,
		"block index to stop syncing",
	)
	checkCmd.Flags().Uint64Var(
		&BlockConcurrency,
		"block-concurrency",
		8,
		"concurrency to use while fetching blocks",
	)
	checkCmd.Flags().Uint64Var(
		&TransactionConcurrency,
		"transaction-concurrency",
		16,
		"concurrency to use while fetching transactions (if required)",
	)
	checkCmd.Flags().Uint64Var(
		&AccountConcurrency,
		"account-concurrency",
		8,
		"concurrency to use while fetching accounts during reconciliation",
	)
	checkCmd.Flags().BoolVar(
		&LogBlocks,
		"log-blocks",
		false,
		"log processed blocks",
	)
	checkCmd.Flags().BoolVar(
		&LogTransactions,
		"log-transactions",
		false,
		"log processed transactions",
	)
	checkCmd.Flags().BoolVar(
		&LogBalanceChanges,
		"log-balance-changes",
		false,
		"log balance changes",
	)
	checkCmd.Flags().BoolVar(
		&LogReconciliations,
		"log-reconciliations",
		false,
		"log balance reconciliations",
	)
	checkCmd.Flags().BoolVar(
		&HaltOnReconciliationError,
		"halt-on-reconciliation-error",
		true,
		`Determines if block processing should halt on a reconciliation
error. It can be beneficial to collect all reconciliation errors or silence
reconciliation errors during development.`,
	)
	checkCmd.Flags().StringVar(
		&ExemptFile,
		"exempt-accounts",
		"",
		`Absolute path to a file listing all accounts to exempt from balance
tracking and reconciliation. Look at the examples directory for an example of
how to structure this file.`,
	)
	checkCmd.Flags().StringVar(
		&BootstrapBalances,
		"bootstrap-balances",
		"",
		`Absolute path to a file used to bootstrap balances before starting syncing.
Populating this value after beginning syncing will return an error.`,
	)
	checkCmd.Flags().BoolVar(
		&LookupBalanceByBlock,
		"lookup-balance-by-block",
		true,
		`When set to true, balances are looked up at the block where a balance
change occurred instead of at the current block. Blockchains that do not support
historical balance lookup should set this to false.`,
	)
	checkCmd.Flags().StringVar(
		&InterestingFile,
		"interesting-accounts",
		"",
		`Absolute path to a file listing all accounts to check on each block. Look
at the examples directory for an example of how to structure this file.`,
	)
}

func runCheckCmd(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())

	exemptAccounts, err := loadAccounts(ExemptFile)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to load exempt accounts", err))
	}

	interestingAccounts, err := loadAccounts(InterestingFile)
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

	// If data directory is not specified, we use a temporary directory
	// and delete its contents when execution is complete.
	if len(DataDir) == 0 {
		tmpDir, err := utils.CreateTempDir()
		if err != nil {
			log.Fatal(fmt.Errorf("%w: unable to create temporary directory", err))
		}
		defer utils.RemoveTempDir(tmpDir)

		DataDir = tmpDir
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

	blockStorageHelper := processor.NewBlockStorageHelper(
		primaryNetwork,
		fetcher,
		LookupBalanceByBlock,
		exemptAccounts,
	)

	blockStorage := storage.NewBlockStorage(ctx, localStore, blockStorageHelper)
	// Bootstrap balances if provided
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

	// Ensure storage is in correct state for starting at index
	if StartIndex != -1 { // attempt to remove blocks from storage (without handling)
		if err = blockStorage.SetNewStartIndex(ctx, StartIndex); err != nil {
			log.Fatal(fmt.Errorf("%w: unable to set new start index", err))
		}
	} else { // attempt to load last processed index
		head, err := blockStorage.GetHeadBlockIdentifier(ctx)
		if err == nil {
			StartIndex = head.Index + 1
		}
	}

	reconcilerHelper := processor.NewReconcilerHelper(
		blockStorage,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		cancel,
		logger,
		HaltOnReconciliationError,
	)

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

	// Load in previous blocks into syncer cache to handle reorgs.
	// If previously processed blocks exist in storage, they are fetched.
	// Otherwise, none are provided to the cache (the syncer will not attempt
	// a reorg if the cache is empty).
	blockCache := []*types.Block{}
	if StartIndex != -1 {
		// This is the case if blocks already in storage or if stateless start
		blockCache = blockStorage.CreateBlockCache(ctx)
	}

	syncer := syncer.New(
		primaryNetwork,
		fetcher,
		syncHandler,
		cancel,
		blockCache,
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
