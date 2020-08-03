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

package tester

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/slowboat0/rosetta-cli/pkg/logger"
	"github.com/slowboat0/rosetta-cli/pkg/processor"
	"github.com/slowboat0/rosetta-cli/pkg/statefulsyncer"
	"github.com/slowboat0/rosetta-cli/pkg/storage"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"golang.org/x/sync/errgroup"
)

const (
	// dataCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	dataCmdName = "check-data"

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

// DataTester coordinates the `check:data` test.
type DataTester struct {
	network           *types.NetworkIdentifier
	database          storage.Database
	config            *configuration.Configuration
	syncer            *statefulsyncer.StatefulSyncer
	reconciler        *reconciler.Reconciler
	logger            *logger.Logger
	counterStorage    *storage.CounterStorage
	reconcilerHandler *processor.ReconcilerHandler
	fetcher           *fetcher.Fetcher
	signalReceived    *bool
	genesisBlock      *types.BlockIdentifier
}

func shouldReconcile(config *configuration.Configuration) bool {
	if config.Data.BalanceTrackingDisabled {
		return false
	}

	if config.Data.ReconciliationDisabled {
		return false
	}

	return true
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

// CloseDatabase closes the database used by DataTester.
func (t *DataTester) CloseDatabase(ctx context.Context) {
	if err := t.database.Close(ctx); err != nil {
		log.Fatalf("%s: error closing database", err.Error())
	}
}

// InitializeData returns a new *DataTester.
func InitializeData(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
	genesisBlock *types.BlockIdentifier,
	interestingAccount *reconciler.AccountCurrency,
	signalReceived *bool,
) *DataTester {
	dataPath, err := utils.CreateCommandPath(config.DataDirectory, dataCmdName, network)
	if err != nil {
		log.Fatalf("%s: cannot create command path", err.Error())
	}

	localStore, err := storage.NewBadgerStorage(ctx, dataPath)
	if err != nil {
		log.Fatalf("%s: unable to initialize database", err.Error())
	}

	exemptAccounts, err := loadAccounts(config.Data.ExemptAccounts)
	if err != nil {
		log.Fatalf("%s: unable to load exempt accounts", err.Error())
	}

	interestingAccounts, err := loadAccounts(config.Data.InterestingAccounts)
	if err != nil {
		log.Fatalf("%s: unable to load interesting accounts", err.Error())
	}

	counterStorage := storage.NewCounterStorage(localStore)
	blockStorage := storage.NewBlockStorage(localStore)
	balanceStorage := storage.NewBalanceStorage(localStore)

	logger := logger.NewLogger(
		counterStorage,
		dataPath,
		config.Data.LogBlocks,
		config.Data.LogTransactions,
		config.Data.LogBalanceChanges,
		config.Data.LogReconciliations,
	)

	reconcilerHelper := processor.NewReconcilerHelper(
		blockStorage,
		balanceStorage,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		!config.Data.IgnoreReconciliationError,
	)

	// Get all previously seen accounts
	seenAccounts, err := balanceStorage.GetAllAccountCurrency(ctx)
	if err != nil {
		log.Fatalf("%s: unable to get previously seen accounts", err.Error())
	}

	r := reconciler.New(
		network,
		reconcilerHelper,
		reconcilerHandler,
		fetcher,
		reconciler.WithActiveConcurrency(int(config.Data.ActiveReconciliationConcurrency)),
		reconciler.WithInactiveConcurrency(int(config.Data.InactiveReconciliationConcurrency)),
		reconciler.WithLookupBalanceByBlock(!config.Data.HistoricalBalanceDisabled),
		reconciler.WithInterestingAccounts(interestingAccounts),
		reconciler.WithSeenAccounts(seenAccounts),
		reconciler.WithDebugLogging(config.Data.LogReconciliations),
		reconciler.WithInactiveFrequency(int64(config.Data.InactiveReconciliationFrequency)),
	)

	blockWorkers := []storage.BlockWorker{}
	if !config.Data.BalanceTrackingDisabled {
		balanceStorageHelper := processor.NewBalanceStorageHelper(
			network,
			fetcher,
			!config.Data.HistoricalBalanceDisabled,
			exemptAccounts,
			false,
		)

		balanceStorageHandler := processor.NewBalanceStorageHandler(
			logger,
			r,
			shouldReconcile(config),
			interestingAccount,
		)

		balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

		// Bootstrap balances if provided
		if len(config.Data.BootstrapBalances) > 0 {
			_, err := blockStorage.GetHeadBlockIdentifier(ctx)
			if err == storage.ErrHeadBlockNotFound {
				err = balanceStorage.BootstrapBalances(
					ctx,
					config.Data.BootstrapBalances,
					genesisBlock,
				)
				if err != nil {
					log.Fatalf("%s: unable to bootstrap balances", err.Error())
				}
			} else {
				log.Println("Skipping balance bootstrapping because already started syncing")
				return nil
			}
		}

		blockWorkers = append(blockWorkers, balanceStorage)
	}

	syncer := statefulsyncer.New(
		ctx,
		network,
		fetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		blockWorkers,
	)

	return &DataTester{
		network:           network,
		database:          localStore,
		config:            config,
		syncer:            syncer,
		reconciler:        r,
		logger:            logger,
		counterStorage:    counterStorage,
		reconcilerHandler: reconcilerHandler,
		fetcher:           fetcher,
		signalReceived:    signalReceived,
		genesisBlock:      genesisBlock,
	}
}

// StartSyncing syncs from startIndex to endIndex.
// If startIndex is -1, it will start from the last
// saved block. If endIndex is -1, it will sync
// continuously (or until an error).
func (t *DataTester) StartSyncing(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	return t.syncer.Sync(ctx, startIndex, endIndex)
}

// StartReconciler starts the reconciler if
// reconciliation is enabled.
func (t *DataTester) StartReconciler(
	ctx context.Context,
) error {
	if !shouldReconcile(t.config) {
		return nil
	}

	return t.reconciler.Reconcile(ctx)
}

// StartPeriodicLogger prints out periodic
// stats about a run of `check:data`.
func (t *DataTester) StartPeriodicLogger(
	ctx context.Context,
) error {
	for ctx.Err() == nil {
		_ = t.logger.LogDataStats(ctx)
		time.Sleep(PeriodicLoggingFrequency)
	}

	// Print stats one last time before exiting
	_ = t.logger.LogDataStats(ctx)

	return ctx.Err()
}

// HandleErr is called when `check:data` returns an error.
// If historical balance lookups are enabled, HandleErr will attempt to
// automatically find any missing balance-changing operations.
func (t *DataTester) HandleErr(ctx context.Context, err error, sigListeners []context.CancelFunc) {
	if *t.signalReceived {
		color.Red("Check halted")
		os.Exit(1)
		return
	}

	if err == nil || err == context.Canceled { // err == context.Canceled when --end
		activeReconciliations, activeErr := t.counterStorage.Get(
			ctx,
			storage.ActiveReconciliationCounter,
		)
		inactiveReconciliations, inactiveErr := t.counterStorage.Get(
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
	if t.reconcilerHandler.InactiveFailure == nil {
		os.Exit(1)
	}

	if t.config.Data.HistoricalBalanceDisabled {
		color.Red(
			"Can't find the block missing operations automatically, please enable --lookup-balance-by-block",
		)
		os.Exit(1)
	}

	t.FindMissingOps(ctx, sigListeners)
}

// FindMissingOps logs the types.BlockIdentifier of a block
// that is missing balance-changing operations for a
// *reconciler.AccountCurrency.
func (t *DataTester) FindMissingOps(ctx context.Context, sigListeners []context.CancelFunc) {
	if t.config.Data.InactiveDiscrepencySearchDisabled {
		color.Red("Search for inactive reconciliation discrepency is disabled")
		os.Exit(1)
	}

	color.Red("Searching for block with missing operations...hold tight")
	badBlock, err := t.recursiveOpSearch(
		ctx,
		&sigListeners,
		t.reconcilerHandler.InactiveFailure,
		t.reconcilerHandler.InactiveFailureBlock.Index-InactiveFailureLookbackWindow,
		t.reconcilerHandler.InactiveFailureBlock.Index,
	)
	if err != nil {
		color.Red("%s: could not find block with missing ops", err.Error())
		os.Exit(1)
	}

	color.Red(
		"Missing ops for %s in block %d:%s",
		types.AccountString(t.reconcilerHandler.InactiveFailure.Account),
		badBlock.Index,
		badBlock.Hash,
	)
	os.Exit(1)
}

func (t *DataTester) recursiveOpSearch(
	ctx context.Context,
	sigListeners *[]context.CancelFunc,
	accountCurrency *reconciler.AccountCurrency,
	startIndex int64,
	endIndex int64,
) (*types.BlockIdentifier, error) {
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
	blockStorage := storage.NewBlockStorage(localStore)
	balanceStorage := storage.NewBalanceStorage(localStore)

	logger := logger.NewLogger(
		counterStorage,
		tmpDir,
		false,
		false,
		false,
		false,
	)

	reconcilerHelper := processor.NewReconcilerHelper(
		blockStorage,
		balanceStorage,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		true, // halt on reconciliation error
	)

	r := reconciler.New(
		t.network,
		reconcilerHelper,
		reconcilerHandler,
		t.fetcher,

		// When using concurrency > 1, we could start looking up balance changes
		// on multiple blocks at once. This can cause us to return the wrong block
		// that is missing operations.
		reconciler.WithActiveConcurrency(1),

		// Do not do any inactive lookups when looking for the block with missing
		// operations.
		reconciler.WithInactiveConcurrency(0),
		reconciler.WithLookupBalanceByBlock(!t.config.Data.HistoricalBalanceDisabled),
		reconciler.WithInterestingAccounts([]*reconciler.AccountCurrency{accountCurrency}),
	)

	balanceStorageHelper := processor.NewBalanceStorageHelper(
		t.network,
		t.fetcher,
		!t.config.Data.HistoricalBalanceDisabled,
		nil,
		false,
	)

	balanceStorageHandler := processor.NewBalanceStorageHandler(
		logger,
		r,
		true,
		accountCurrency,
	)

	balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

	syncer := statefulsyncer.New(
		ctx,
		t.network,
		t.fetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]storage.BlockWorker{balanceStorage},
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.Reconcile(ctx)
	})

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

	if *t.signalReceived {
		return nil, errors.New("Search for block with missing ops halted")
	}

	if err == nil || err == context.Canceled {
		newStart := startIndex - InactiveFailureLookbackWindow
		if newStart < t.genesisBlock.Index {
			newStart = t.genesisBlock.Index
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

		return t.recursiveOpSearch(
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
