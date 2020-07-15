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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/processor"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
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

	// InactiveFailureLookbackWindow is the size of each window to check
	// for missing ops. If a block with missing ops is not found in this
	// window, another window is created with the preceding
	// InactiveFailureLookbackWindow blocks (this process continues
	// until the client halts the search or the block is found).
	InactiveFailureLookbackWindow = 250

	// PeriodicLoggingFrequency is the frequency that stats are printed
	// to the terminal.
	//
	// TODO: make configurable
	PeriodicLoggingFrequency = 10 * time.Second
)

var (
	checkCmd = &cobra.Command{
		Use:   "check",
		Short: "Check the correctness of a Rosetta Node API Server",
		Long: `Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off if you specify
some --data-dir. Otherwise, it will create a new temporary directory and start
again from the genesis block. If you want to discard some number of blocks
populate the --start flag with some block index. Starting from a given index
can be useful to debug a small range of blocks for issues but it is highly
recommended you sync from start to finish to ensure all correctness checks
are performed.

By default, account balances are looked up at specific heights (instead of
only at the current block). If your node does not support this functionality
set --lookup-balance-by-block to false. This will make reconciliation much
less efficient but it will still work.

If check fails due to an INACTIVE reconciliation error (balance changed without
any corresponding operation), the cli will automatically try to find the block
missing an operation. If --lookup-balance-by-block is not enabled, this automatic
debugging tool does not work.

To debug an INACTIVE account reconciliation error without --lookup-balance-by-block, set the
--interesting-accounts flag to the absolute path of a JSON file containing
accounts that will be actively checked for balance changes at each block. This
will return an error at the block where a balance change occurred with no
corresponding operations.

If your blockchain has a genesis allocation of funds and you set
--lookup-balance-by-block to false, you must provide an
absolute path to a JSON file containing initial balances with the
--bootstrap-balances flag. You can look at the examples folder for an example
of what one of these files looks like.`,
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

	// ActiveReconciliationConcurrency is the concurrency to use
	// while fetching accounts during active reconciliation.
	ActiveReconciliationConcurrency uint64

	// InactiveReconciliationConcurrency is the concurrency to use
	// while fetching accounts during inactive reconciliation.
	InactiveReconciliationConcurrency uint64

	// InactiveReconciliationFrequency is the number of blocks
	// to wait between inactive reconiliations on each account.
	InactiveReconciliationFrequency uint64

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

	// signalReceived is set to true when a signal causes us to exit. This makes
	// determining the error message to show on exit much more easy.
	signalReceived = false
)

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
		&ActiveReconciliationConcurrency,
		"active-reconciliation-concurrency",
		8,
		"concurrency to use while fetching accounts during active reconciliation",
	)
	checkCmd.Flags().Uint64Var(
		&InactiveReconciliationConcurrency,
		"inactive-reconciliation-concurrency",
		4,
		"concurrency to use while fetching accounts during inactive reconciliation",
	)
	checkCmd.Flags().Uint64Var(
		&InactiveReconciliationFrequency,
		"inactive-reconciliation-frequency",
		250,
		"the number of blocks to wait between inactive reconiliations on each account",
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

// loadAccounts is a utility function to parse the []*reconciler.AccountCurrency
// in a file.
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

	log.Printf(
		"Found %d accounts at %s: %s\n",
		len(accounts),
		filePath,
		types.PrettyPrintStruct(accounts),
	)

	return accounts, nil
}

// findMissingOps returns the types.BlockIdentifier of a block
// that is missing balance-changing operations for a
// *reconciler.AccountCurrency.
func findMissingOps(
	ctx context.Context,
	sigListeners *[]context.CancelFunc,
	accountCurrency *reconciler.AccountCurrency,
	startIndex int64,
	endIndex int64,
) (*types.BlockIdentifier, error) {
	fetcher := fetcher.New(
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
		fetcher.WithRetryElapsedTime(ExtendedRetryElapsedTime),
	)

	primaryNetwork, networkStatus, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to initialize asserter", err)
	}

	// To cancel all execution, need to call multiple cancel functions.
	ctx, cancel := context.WithCancel(ctx)
	*sigListeners = append(*sigListeners, cancel)

	// Always use a temporary directory to find missing ops
	tmpDir, err := utils.CreateTempDir()
	if err != nil {
		return nil, fmt.Errorf("%w: unable to create temporary directory", err)
	}
	defer utils.RemoveTempDir(tmpDir)

	localStore, err := storage.NewBadgerStorage(ctx, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to initialize database", err)
	}

	counterStorage := storage.NewCounterStorage(localStore)

	logger := logger.NewLogger(
		counterStorage,
		tmpDir,
		false,
		false,
		false,
		false,
	)

	blockStorageHelper := processor.NewBlockStorageHelper(
		primaryNetwork,
		fetcher,
		LookupBalanceByBlock,
		nil,
	)

	blockStorage := storage.NewBlockStorage(localStore, blockStorageHelper)

	// Ensure storage is in correct state for starting at index
	if err = blockStorage.SetNewStartIndex(ctx, startIndex); err != nil {
		return nil, fmt.Errorf("%w: unable to set new start index", err)
	}

	reconcilerHelper := processor.NewReconcilerHelper(
		blockStorage,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		true, // halt on reconciliation error
	)

	r := reconciler.New(
		primaryNetwork,
		reconcilerHelper,
		reconcilerHandler,
		fetcher,

		// When using concurrency > 1, we could start looking up balance changes
		// on multiple blocks at once. This can cause us to return the wrong block
		// that is missing operations.
		reconciler.WithActiveConcurrency(1),

		// Do not do any inactive lookups when looking for the block with missing
		// operations.
		reconciler.WithInactiveConcurrency(0),
		reconciler.WithLookupBalanceByBlock(LookupBalanceByBlock),
		reconciler.WithInterestingAccounts([]*reconciler.AccountCurrency{accountCurrency}),
	)

	syncerHandler := processor.NewSyncerHandler(
		blockStorage,
		logger,
		r,
		fetcher,
		accountCurrency,
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	syncer := syncer.New(
		primaryNetwork,
		fetcher,
		syncerHandler,
		cancel,
		nil,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			startIndex,
			endIndex,
		)
	})

	err = g.Wait()

	// Close database before starting another search, otherwise we will
	// have n databases open when we find the offending block.
	if storageErr := localStore.Close(ctx); storageErr != nil {
		return nil, fmt.Errorf("%w: unable to close database", storageErr)
	}

	if signalReceived {
		return nil, errors.New("Search for block with missing ops halted")
	}

	if err == nil || err == context.Canceled {
		newStart := startIndex - InactiveFailureLookbackWindow
		if newStart < networkStatus.GenesisBlockIdentifier.Index {
			newStart = networkStatus.GenesisBlockIdentifier.Index
		}

		newEnd := endIndex - InactiveFailureLookbackWindow
		if newEnd <= newStart {
			return nil, fmt.Errorf(
				"Next window to check has start index %d <= end index %d",
				newStart,
				newEnd,
			)
		}

		color.Red(
			"Unable to find missing ops in block range %d-%d, now searching %d-%d",
			startIndex, endIndex,
			newStart,
			newEnd,
		)

		return findMissingOps(
			// We need to use new context for each invocation because the syncer
			// cancels the provided context when it reaches the end of a syncing
			// window.
			context.Background(),
			sigListeners,
			accountCurrency,
			startIndex-InactiveFailureLookbackWindow,
			endIndex-InactiveFailureLookbackWindow,
		)
	}

	if reconcilerHandler.ActiveFailureBlock == nil {
		return nil, errors.New("unable to find missing ops")
	}

	return reconcilerHandler.ActiveFailureBlock, nil
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
		log.Fatal(fmt.Errorf("%w: unable to initialize database", err))
	}
	defer localStore.Close(ctx)

	counterStorage := storage.NewCounterStorage(localStore)

	logger := logger.NewLogger(
		counterStorage,
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

	blockStorage := storage.NewBlockStorage(localStore, blockStorageHelper)

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

	// Get all previously seen accounts
	seenAccounts, err := blockStorage.GetAllAccountCurrency(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to get previously seen accounts", err))
	}

	reconcilerHelper := processor.NewReconcilerHelper(
		blockStorage,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		HaltOnReconciliationError,
	)

	r := reconciler.New(
		primaryNetwork,
		reconcilerHelper,
		reconcilerHandler,
		fetcher,
		reconciler.WithActiveConcurrency(int(ActiveReconciliationConcurrency)),
		reconciler.WithInactiveConcurrency(int(InactiveReconciliationConcurrency)),
		reconciler.WithLookupBalanceByBlock(LookupBalanceByBlock),
		reconciler.WithInterestingAccounts(interestingAccounts),
		reconciler.WithSeenAccounts(seenAccounts),
		reconciler.WithDebugLogging(LogReconciliations),
		reconciler.WithInactiveFrequency(int64(InactiveReconciliationFrequency)),
	)

	syncerHandler := processor.NewSyncerHandler(
		blockStorage,
		logger,
		r,
		fetcher,
		nil,
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for ctx.Err() == nil {
			_ = logger.LogCounterStorage(ctx)
			time.Sleep(PeriodicLoggingFrequency)
		}

		return nil
	})

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	// Load in previous blocks into syncer cache to handle reorgs.
	// If previously processed blocks exist in storage, they are fetched.
	// Otherwise, none are provided to the cache (the syncer will not attempt
	// a reorg if the cache is empty).
	pastBlocks := []*types.BlockIdentifier{}
	if StartIndex != -1 {
		// This is the case if blocks already in storage or if stateless start
		pastBlocks = blockStorage.CreateBlockCache(ctx)
	}

	syncer := syncer.New(
		primaryNetwork,
		fetcher,
		syncerHandler,
		cancel, // needed to exit without error when --end flag provided
		pastBlocks,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			StartIndex,
			EndIndex,
		)
	})

	// Handle OS signals so we can ensure we close database
	// correctly. We call multiple sigListeners because we
	// may need to cancel more than 1 context.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sigListeners := []context.CancelFunc{cancel}
	go func() {
		sig := <-sigs
		color.Red("Received signal: %s", sig)
		signalReceived = true
		for _, listener := range sigListeners {
			listener()
		}
	}()

	handleCheckResult(g, counterStorage, reconcilerHandler, sigListeners)
}

// handleCheckResult interprets the check exectution result
// and terminates with the correct exit status.
func handleCheckResult(
	g *errgroup.Group,
	counterStorage *storage.CounterStorage,
	reconcilerHandler *processor.ReconcilerHandler,
	sigListeners []context.CancelFunc,
) {
	// Initialize new context because calling context
	// will no longer be usable when after termination.
	ctx := context.Background()

	err := g.Wait()
	if signalReceived {
		color.Red("Check halted")
		os.Exit(1)
		return
	}

	if err == nil || err == context.Canceled { // err == context.Canceled when --end
		activeReconciliations, activeErr := counterStorage.Get(
			ctx,
			storage.ActiveReconciliationCounter,
		)
		inactiveReconciliations, inactiveErr := counterStorage.Get(
			ctx,
			storage.InactiveReconciliationCounter,
		)

		if activeErr != nil || inactiveErr != nil ||
			new(big.Int).Add(activeReconciliations, inactiveReconciliations).Sign() != 0 {
			color.Green("Check succeeded")
		} else { // warn caller when check succeeded but no reconciliations performed (as issues may still exist)
			color.Yellow("Check succeeded, however, no reconciliations were performed!")
		}
		os.Exit(0)
	}

	color.Red("Check failed: %s", err.Error())
	if reconcilerHandler.InactiveFailure == nil {
		os.Exit(1)
	}

	if !LookupBalanceByBlock {
		color.Red(
			"Can't find the block missing operations automatically, please enable --lookup-balance-by-block",
		)
		os.Exit(1)
	}

	color.Red("Searching for block with missing operations...hold tight")
	badBlock, err := findMissingOps(
		ctx,
		&sigListeners,
		reconcilerHandler.InactiveFailure,
		reconcilerHandler.InactiveFailureBlock.Index-InactiveFailureLookbackWindow,
		reconcilerHandler.InactiveFailureBlock.Index,
	)
	if err != nil {
		color.Red("%s: could not find block with missing ops", err.Error())
		os.Exit(1)
	}

	color.Red(
		"Missing ops for %s in block %d:%s",
		types.AccountString(reconcilerHandler.InactiveFailure.Account),
		badBlock.Index,
		badBlock.Hash,
	)
	os.Exit(1)
}
