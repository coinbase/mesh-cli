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
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
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
	checkDataCmd = &cobra.Command{
		Use:   "check:data",
		Short: "Check the correctness of a Rosetta Data API Implementation",
		Long: `Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off if you specify
some data directory. Otherwise, it will create a new temporary directory and start
again from the genesis block. If you want to discard some number of blocks
populate the --start flag with some block index. Starting from a given index
can be useful to debug a small range of blocks for issues but it is highly
recommended you sync from start to finish to ensure all correctness checks
are performed.

By default, account balances are looked up at specific heights (instead of
only at the current block). If your node does not support this functionality
set historical balance disabled to true. This will make reconciliation much
less efficient but it will still work.

If check fails due to an INACTIVE reconciliation error (balance changed without
any corresponding operation), the cli will automatically try to find the block
missing an operation. If historical balance disabled is true, this automatic
debugging tool does not work.

To debug an INACTIVE account reconciliation error without historical balance lookup,
set the interesting accunts to the path of a JSON file containing
accounts that will be actively checked for balance changes at each block. This
will return an error at the block where a balance change occurred with no
corresponding operations.

If your blockchain has a genesis allocation of funds and you set
historical balance disabled to true, you must provide an
absolute path to a JSON file containing initial balances with the
bootstrap balance config. You can look at the examples folder for an example
of what one of these files looks like.`,
		Run: runCheckDataCmd,
	}

	// StartIndex is the block index to start syncing.
	StartIndex int64

	// EndIndex is the block index to stop syncing.
	EndIndex int64
)

func init() {
	checkDataCmd.Flags().Int64Var(
		&StartIndex,
		"start",
		-1,
		"block index to start syncing",
	)
	checkDataCmd.Flags().Int64Var(
		&EndIndex,
		"end",
		-1,
		"block index to stop syncing",
	)
}

// loadAccounts is a utility function to parse the []*reconciler.AccountCurrency
// in a file.
func loadAccounts(filePath string) ([]*reconciler.AccountCurrency, error) {
	if len(filePath) == 0 {
		return []*reconciler.AccountCurrency{}, nil
	}

	accounts := []*reconciler.AccountCurrency{}
	if err := utils.LoadAndParse(filePath, &accounts); err != nil {
		return nil, fmt.Errorf("%w: unable to open account file", err)
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
		Config.OnlineURL,
		fetcher.WithBlockConcurrency(Config.Data.BlockConcurrency),
		fetcher.WithTransactionConcurrency(Config.Data.TransactionConcurrency),
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
		!Config.Data.HistoricalBalanceDisabled,
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
		reconciler.WithLookupBalanceByBlock(!Config.Data.HistoricalBalanceDisabled),
		reconciler.WithInterestingAccounts([]*reconciler.AccountCurrency{accountCurrency}),
	)

	syncerHandler := processor.NewCheckDataHandler(
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

	if SignalReceived {
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

func runCheckDataCmd(cmd *cobra.Command, args []string) {
	ensureDataDirectoryExists()
	ctx, cancel := context.WithCancel(context.Background())

	exemptAccounts, err := loadAccounts(Config.Data.ExemptAccounts)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to load exempt accounts", err))
	}

	interestingAccounts, err := loadAccounts(Config.Data.InterestingAccounts)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to load interesting accounts", err))
	}

	fetcher := fetcher.New(
		Config.OnlineURL,
		fetcher.WithBlockConcurrency(Config.Data.BlockConcurrency),
		fetcher.WithTransactionConcurrency(Config.Data.TransactionConcurrency),
		fetcher.WithRetryElapsedTime(ExtendedRetryElapsedTime),
	)

	// TODO: sync and reconcile on subnetworks, if they exist.
	primaryNetwork, networkStatus, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to initialize asserter", err))
	}

	localStore, err := storage.NewBadgerStorage(ctx, Config.DataDirectory)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to initialize database", err))
	}
	defer localStore.Close(ctx)

	counterStorage := storage.NewCounterStorage(localStore)

	logger := logger.NewLogger(
		counterStorage,
		Config.DataDirectory,
		Config.Data.LogBlocks,
		Config.Data.LogTransactions,
		Config.Data.LogBalanceChanges,
		Config.Data.LogReconciliations,
	)

	blockStorageHelper := processor.NewBlockStorageHelper(
		primaryNetwork,
		fetcher,
		!Config.Data.HistoricalBalanceDisabled,
		exemptAccounts,
	)

	blockStorage := storage.NewBlockStorage(localStore, blockStorageHelper)

	// Bootstrap balances if provided
	if len(Config.Data.BootstrapBalances) > 0 {
		err = blockStorage.BootstrapBalances(
			ctx,
			Config.Data.BootstrapBalances,
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
		!Config.Data.IgnoreReconciliationError,
	)

	r := reconciler.New(
		primaryNetwork,
		reconcilerHelper,
		reconcilerHandler,
		fetcher,
		reconciler.WithActiveConcurrency(int(Config.Data.ActiveReconciliationConcurrency)),
		reconciler.WithInactiveConcurrency(int(Config.Data.InactiveReconciliationConcurrency)),
		reconciler.WithLookupBalanceByBlock(!Config.Data.HistoricalBalanceDisabled),
		reconciler.WithInterestingAccounts(interestingAccounts),
		reconciler.WithSeenAccounts(seenAccounts),
		reconciler.WithDebugLogging(Config.Data.LogReconciliations),
		reconciler.WithInactiveFrequency(int64(Config.Data.InactiveReconciliationFrequency)),
	)

	syncerHandler := processor.NewCheckDataHandler(
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

		// Print stats one last time before exiting
		_ = logger.LogCounterStorage(ctx)

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

	sigListeners := []context.CancelFunc{cancel}
	go handleSignals(sigListeners)

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
	if SignalReceived {
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

	if Config.Data.HistoricalBalanceDisabled {
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
