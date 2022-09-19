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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
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

	// periodicLoggingSeconds is the frequency to print stats in seconds.
	periodicLoggingSeconds = 10

	// PeriodicLoggingFrequency is the frequency that stats are printed
	// to the terminal.
	PeriodicLoggingFrequency = periodicLoggingSeconds * time.Second

	// EndAtTipCheckInterval is the frequency that EndAtTip condition
	// is evaluated
	EndAtTipCheckInterval = 10 * time.Second

	//MinTableSize unit is GB
	MinTableSize = int64(1)
	
	//MaxTableSize unit is GB
	MaxTableSize = int64(100)

	//MinTableSize unit is MB
	MinValueLogFileSize = int64(128)

	//MaxTableSize unit is MB
	MaxValueLogFileSize = int64(2048)
)

var _ http.Handler = (*DataTester)(nil)
var _ statefulsyncer.PruneHelper = (*DataTester)(nil)

// DataTester coordinates the `check:data` test.
type DataTester struct {
	network                     *types.NetworkIdentifier
	database                    database.Database
	config                      *configuration.Configuration
	syncer                      *statefulsyncer.StatefulSyncer
	reconciler                  *reconciler.Reconciler
	logger                      *logger.Logger
	balanceStorage              *modules.BalanceStorage
	blockStorage                *modules.BlockStorage
	counterStorage              *modules.CounterStorage
	reconcilerHandler           *processor.ReconcilerHandler
	fetcher                     *fetcher.Fetcher
	signalReceived              *bool
	genesisBlock                *types.BlockIdentifier
	cancel                      context.CancelFunc
	historicalBalanceEnabled    bool
	parser                      *parser.Parser
	forceInactiveReconciliation *bool

	endCondition       configuration.CheckDataEndCondition
	endConditionDetail string
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

// loadAccounts is a utility function to parse the []*types.AccountCurrency
// in a file.
func loadAccounts(filePath string) ([]*types.AccountCurrency, error) {
	if len(filePath) == 0 {
		return []*types.AccountCurrency{}, nil
	}

	accounts := []*types.AccountCurrency{}
	if err := utils.LoadAndParse(filePath, &accounts); err != nil {
		return nil, fmt.Errorf("unable to load and parse %s: %w", filePath, err)
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
		log.Fatalf("error closing database: %s", err.Error())
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
	interestingAccount *types.AccountCurrency,
	signalReceived *bool,
) (*DataTester, error) {
	dataPath, err := utils.CreateCommandPath(config.DataDirectory, dataCmdName, network)
	if err != nil {
		return nil, fmt.Errorf("failed to create command path: %w", err)
	}

	opts := []database.BadgerOption{}
	dataPathBackup := dataPath

	if config.AllInMemoryEnabled {
		opts = append(
			opts,
			database.WithCustomSettings(database.AllInMemoryBadgerOptions(dataPath)),
			database.WithoutCompression(),
		)
		// for all in memory mode, the path need to be "", as badgerDB will not write to disk
		dataPathBackup = ""
	} else {
		if config.CompressionDisabled {
			opts = append(opts, database.WithoutCompression())
		}
		if config.MemoryLimitDisabled {
			opts = append(
				opts,
				database.WithCustomSettings(database.PerformanceBadgerOptions(dataPath)),
			)
		}
	}

	// If we enable all-in-memory or L0-in-memory mode, badger DB's TableSize and ValueLogFileSize will change
	// according to users config. tableSize means the LSM table size, when the table more than the tableSize,
	// will trigger a compact.
	// In default mode, we will not change the badger DB's TableSize and ValueLogFileSize for limiting memory usage
	if config.AllInMemoryEnabled || config.MemoryLimitDisabled {
		if config.TableSize != nil {
			if *config.TableSize >= MinTableSize && *config.TableSize <= MaxTableSize {
				opts = append(
					opts,
					database.WithTableSize(*config.TableSize),
				)
			}
		}
		if config.ValueLogFileSize != nil {
			if *config.TableSize >= MinValueLogFileSize && *config.TableSize <= MinValueLogFileSize {
				opts = append(
					opts,
					database.WithValueLogFileSize(*config.TableSize),
				)
			}
		}
	}

	localStore, err := database.NewBadgerDatabase(ctx, dataPathBackup, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize database: %w", err)
	}

	exemptAccounts, err := loadAccounts(config.Data.ExemptAccounts)
	if err != nil {
		return nil, fmt.Errorf("unable to load exempt accounts: %w", err)
	}

	interestingAccounts, err := loadAccounts(config.Data.InterestingAccounts)
	if err != nil {
		return nil, fmt.Errorf("unable to load interesting accounts: %w", err)
	}

	counterStorage := modules.NewCounterStorage(localStore)
	blockStorage := modules.NewBlockStorage(localStore, config.SerialBlockWorkers)
	balanceStorage := modules.NewBalanceStorage(localStore)

	logger, err := logger.NewLogger(
		dataPath,
		config.Data.LogBlocks,
		config.Data.LogTransactions,
		config.Data.LogBalanceChanges,
		config.Data.LogReconciliations,
		logger.Data,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize logger with error: %w", err)
	}

	var forceInactiveReconciliation bool
	reconcilerHelper := processor.NewReconcilerHelper(
		config,
		network,
		fetcher,
		localStore,
		blockStorage,
		balanceStorage,
		&forceInactiveReconciliation,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		counterStorage,
		balanceStorage,
		!config.Data.IgnoreReconciliationError,
	)

	// Get all previously seen accounts
	seenAccounts, err := balanceStorage.GetAllAccountCurrency(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get previously seen accounts: %w", err)
	}

	networkOptions, fetchErr := fetcher.NetworkOptionsRetry(ctx, network, nil)
	if fetchErr != nil {
		log.Fatalf("unable to get network options: %s", fetchErr.Err.Error())
	}

	if len(networkOptions.Allow.BalanceExemptions) > 0 && config.Data.InitialBalanceFetchDisabled {
		return nil, fmt.Errorf("found balance exemptions but initial balance fetch disabled")
	}

	parser := parser.New(
		fetcher.Asserter,
		nil,
		networkOptions.Allow.BalanceExemptions,
	)

	// Determine if we should perform historical balance lookups
	var historicalBalanceEnabled bool
	if config.Data.HistoricalBalanceDisabled != nil {
		historicalBalanceEnabled = !*config.Data.HistoricalBalanceDisabled
	} else { // we must look it up
		historicalBalanceEnabled = networkOptions.Allow.HistoricalBalanceLookup
	}

	rOpts := []reconciler.Option{
		reconciler.WithActiveConcurrency(int(config.Data.ActiveReconciliationConcurrency)),
		reconciler.WithInactiveConcurrency(int(config.Data.InactiveReconciliationConcurrency)),
		reconciler.WithInterestingAccounts(interestingAccounts),
		reconciler.WithSeenAccounts(seenAccounts),
		reconciler.WithInactiveFrequency(int64(config.Data.InactiveReconciliationFrequency)),
		reconciler.WithBalancePruning(),
	}
	if config.Data.ReconcilerActiveBacklog != nil {
		rOpts = append(rOpts, reconciler.WithBacklogSize(*config.Data.ReconcilerActiveBacklog))
	}
	if historicalBalanceEnabled {
		rOpts = append(rOpts, reconciler.WithLookupBalanceByBlock())
	}
	if config.Data.LogReconciliations {
		rOpts = append(rOpts, reconciler.WithDebugLogging())
	}

	r := reconciler.New(
		reconcilerHelper,
		reconcilerHandler,
		parser,
		rOpts...,
	)

	blockWorkers := []modules.BlockWorker{counterStorage}
	if !config.Data.BalanceTrackingDisabled {
		balanceStorageHelper := processor.NewBalanceStorageHelper(
			network,
			fetcher,
			counterStorage,
			historicalBalanceEnabled,
			exemptAccounts,
			false,
			networkOptions.Allow.BalanceExemptions,
			config.Data.InitialBalanceFetchDisabled,
		)

		balanceStorageHandler := processor.NewBalanceStorageHandler(
			logger,
			r,
			counterStorage,
			shouldReconcile(config),
			interestingAccount,
		)

		balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

		blockWorkers = append(blockWorkers, balanceStorage)

		// Bootstrap balances, if provided. We need to do before initializing
		// the reconciler otherwise we won't reconcile bootstrapped accounts
		// until rosetta-cli restart.
		//
		// We need to do this after instantiating the balance storage handler
		// because it is invoked within BootstrapBalances.
		if len(config.Data.BootstrapBalances) > 0 {
			_, err := blockStorage.GetHeadBlockIdentifier(ctx)
			switch {
			case err == storageErrs.ErrHeadBlockNotFound:
				err = balanceStorage.BootstrapBalances(
					ctx,
					config.Data.BootstrapBalances,
					genesisBlock,
				)
				if err != nil {
					return nil, fmt.Errorf("unable to bootstrap balances: %w", err)
				}
			case err != nil:
				return nil, fmt.Errorf("unable to get head block identifier: %w", err)
			default:
				log.Println("Skipping balance bootstrapping because already started syncing")
			}
		}
	}

	if !config.Data.CoinTrackingDisabled {
		coinStorageHelper := processor.NewCoinStorageHelper(blockStorage)
		coinStorage := modules.NewCoinStorage(localStore, coinStorageHelper, fetcher.Asserter)

		blockWorkers = append(blockWorkers, coinStorage)
	}

	statefulSyncerOptions := []statefulsyncer.Option{
		statefulsyncer.WithCacheSize(syncer.DefaultCacheSize),
		statefulsyncer.WithMaxConcurrency(config.MaxSyncConcurrency),
		statefulsyncer.WithPastBlockLimit(config.MaxReorgDepth),
		statefulsyncer.WithSeenConcurrency(int64(config.SeenBlockWorkers)),
	}
	if config.Data.PruningFrequency != nil {
		statefulSyncerOptions = append(
			statefulSyncerOptions,
			statefulsyncer.WithPruneSleepTime(*config.Data.PruningFrequency),
		)
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
		statefulSyncerOptions...,
	)

	return &DataTester{
		network:                     network,
		database:                    localStore,
		config:                      config,
		syncer:                      syncer,
		cancel:                      cancel,
		reconciler:                  r,
		logger:                      logger,
		balanceStorage:              balanceStorage,
		blockStorage:                blockStorage,
		counterStorage:              counterStorage,
		reconcilerHandler:           reconcilerHandler,
		fetcher:                     fetcher,
		signalReceived:              signalReceived,
		genesisBlock:                genesisBlock,
		historicalBalanceEnabled:    historicalBalanceEnabled,
		parser:                      parser,
		forceInactiveReconciliation: &forceInactiveReconciliation,
	}, nil
}

// StartSyncing syncs from startIndex to endIndex.
// If startIndex is -1, it will start from the last
// saved block. If endIndex is -1, it will sync
// continuously (or until an error).
func (t *DataTester) StartSyncing(
	ctx context.Context,
) error {
	startIndex := int64(-1)
	if t.config.Data.StartIndex != nil {
		startIndex = *t.config.Data.StartIndex
	}

	endIndex := int64(-1)
	if t.config.Data.EndConditions != nil && t.config.Data.EndConditions.Index != nil {
		endIndex = *t.config.Data.EndConditions.Index
	}

	return t.syncer.Sync(ctx, startIndex, endIndex)
}

// StartPruning attempts to prune block storage
// every 10 seconds.
func (t *DataTester) StartPruning(
	ctx context.Context,
) error {
	if t.config.Data.PruningBlockDisabled {
		return nil
	}

	return t.syncer.Prune(ctx, t)
}

// StartReconcilerCountUpdater attempts to periodically
// write cached reconciler count updates to storage.
func (t *DataTester) StartReconcilerCountUpdater(
	ctx context.Context,
) error {
	return t.reconcilerHandler.Updater(ctx)
}

// PruneableIndex is the index that is
// safe for pruning.
func (t *DataTester) PruneableIndex(
	ctx context.Context,
	headIndex int64,
) (int64, error) {
	// We don't need blocks to exist to reconcile
	// balances at their index.
	//
	// It is ok if the returned value here is negative.
	return headIndex - int64(t.config.MaxReorgDepth), nil
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
	tc := time.NewTicker(PeriodicLoggingFrequency)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tc.C:
			// Update the elapsed time in counter storage so that
			// we can log metrics about the current check:data run.
			_, _ = t.counterStorage.Update(
				ctx,
				results.TimeElapsedCounter,
				big.NewInt(periodicLoggingSeconds),
			)

			status := results.ComputeCheckDataStatus(
				ctx,
				t.blockStorage,
				t.counterStorage,
				t.balanceStorage,
				t.fetcher,
				t.config.Network,
				t.reconciler,
			)
			t.logger.LogDataStatus(ctx, status)
		}
	}
}

// ServeHTTP serves a CheckDataStatus response on all paths.
func (t *DataTester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	status := results.ComputeCheckDataStatus(
		r.Context(),
		t.blockStorage,
		t.counterStorage,
		t.balanceStorage,
		t.fetcher,
		t.network,
		t.reconciler,
	)

	if err := json.NewEncoder(w).Encode(status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// syncedStatus returns a boolean indicating if we are synced to tip and
// the last synced block.
func (t *DataTester) syncedStatus(ctx context.Context) (bool, int64, error) {
	atTip, blockIdentifier, err := utils.CheckStorageTip(
		ctx,
		t.network,
		t.config.TipDelay,
		t.fetcher,
		t.blockStorage,
	)
	if err != nil {
		return false, -1, fmt.Errorf("failed to check storage tip: %w", err)
	}

	var blockIndex int64 = -1

	if blockIdentifier != nil {
		blockIndex = blockIdentifier.Index
	}

	return atTip, blockIndex, nil
}

// EndAtTipLoop runs a loop that evaluates end condition EndAtTip
func (t *DataTester) EndAtTipLoop(
	ctx context.Context,
) {
	tc := time.NewTicker(EndAtTipCheckInterval)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tc.C:
			atTip, blockIndex, err := t.syncedStatus(ctx)
			if err != nil {
				log.Printf(
					"unable to evaluate if syncer is at tip: %s",
					err.Error(),
				)
				continue
			}

			if atTip {
				t.endCondition = configuration.TipEndCondition
				t.endConditionDetail = fmt.Sprintf(
					"Tip: %d",
					blockIndex,
				)
				t.cancel()
				return
			}
		}
	}
}

// EndReconciliationCoverage runs a loop that evaluates ReconciliationEndCondition
func (t *DataTester) EndReconciliationCoverage( // nolint:gocognit
	ctx context.Context,
	reconciliationCoverage *configuration.ReconciliationCoverage,
) {
	tc := time.NewTicker(EndAtTipCheckInterval)
	defer tc.Stop()

	firstTipIndex := int64(-1)

	for {
		select {
		case <-ctx.Done():
			return

		case <-tc.C:
			atTip, blockIndex, err := t.syncedStatus(ctx)
			if err != nil {
				log.Printf(
					"unable to evaluate syncer height or if at tip: %s",
					err.Error(),
				)
				continue
			}

			// Check if we are at tip and set tip height if fromTip is true.
			if reconciliationCoverage.Tip || reconciliationCoverage.FromTip {
				// If we fall behind tip, we must reset the firstTipIndex.
				var disableForceReconciliation bool
				if !atTip {
					disableForceReconciliation = true
					firstTipIndex = int64(-1)
					continue
				}

				// forceInactiveReconciliation should NEVER be nil
				// by this point but we check just to be sure.
				if t.forceInactiveReconciliation != nil {
					*t.forceInactiveReconciliation = !disableForceReconciliation
				}

				// Once at tip, we want to consider
				// coverage. It is not feasible that we could
				// get high reconciliation coverage at the tip
				// block, so we take the range from when first
				// at tip to the current block.
				if firstTipIndex < 0 {
					firstTipIndex = blockIndex
				}
			}

			// minIndex is the greater of firstTipIndex
			// and reconciliationCoverage.Index
			minIndex := firstTipIndex

			// Check if at required minimum index
			if reconciliationCoverage.Index != nil {
				if *reconciliationCoverage.Index < blockIndex {
					continue
				}

				// Override the firstTipIndex if reconciliationCoverage.Index
				// is greater
				if *reconciliationCoverage.Index > minIndex {
					minIndex = *reconciliationCoverage.Index
				}
			}

			// Check if all accounts reconciled at index (+1). If last index reconciled
			// is less than the minimum allowed index but the QueueSize is 0, then
			// we consider the reconciler to be caught up.
			if t.reconciler.LastIndexReconciled() <= minIndex && t.reconciler.QueueSize() > 0 {
				continue
			}

			// Check if account count is above minimum index
			if reconciliationCoverage.AccountCount != nil {
				allAccounts, err := t.balanceStorage.GetAllAccountCurrency(ctx)
				if err != nil {
					log.Printf(
						"unable to get account count: %s",
						err.Error(),
					)
					continue
				}

				if int64(len(allAccounts)) < *reconciliationCoverage.AccountCount {
					continue
				}
			}

			coverageIndex := int64(0)
			if reconciliationCoverage.FromTip {
				coverageIndex = firstTipIndex
			}

			coverage, err := t.balanceStorage.ReconciliationCoverage(ctx, coverageIndex)
			if err != nil {
				log.Printf(
					"unable to get reconciliation coverage: %s",
					err.Error(),
				)
				continue
			}

			if coverage >= reconciliationCoverage.Coverage {
				t.endCondition = configuration.ReconciliationCoverageEndCondition
				t.endConditionDetail = fmt.Sprintf(
					"Coverage: %f%%",
					coverage*utils.OneHundred,
				)
				t.cancel()
				return
			}

			color.Cyan(fmt.Sprintf(
				"[END CONDITIONS] Waiting for reconciliation coverage after block %d (%f%%) to surpass requirement (%f%%)",
				firstTipIndex,
				coverage*utils.OneHundred,
				reconciliationCoverage.Coverage*utils.OneHundred,
			))
		}
	}
}

// EndDurationLoop runs a loop that evaluates end condition EndDuration.
func (t *DataTester) EndDurationLoop(
	ctx context.Context,
	duration time.Duration,
) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
			t.endCondition = configuration.DurationEndCondition
			t.endConditionDetail = fmt.Sprintf(
				"Seconds: %d",
				int(duration.Seconds()),
			)
			t.cancel()
			return
		}
	}
}

// WatchEndConditions starts go routines to watch the end conditions
func (t *DataTester) WatchEndConditions(
	ctx context.Context,
) error {
	endConds := t.config.Data.EndConditions
	if endConds == nil {
		return nil
	}

	if endConds.Tip != nil && *endConds.Tip {
		// runs a go routine that ends when reaching tip
		go t.EndAtTipLoop(ctx)
	}

	if endConds.Duration != nil && *endConds.Duration != 0 {
		// runs a go routine that ends after a duration
		go t.EndDurationLoop(ctx, time.Duration(*endConds.Duration)*time.Second)
	}

	if endConds.ReconciliationCoverage != nil {
		go t.EndReconciliationCoverage(ctx, endConds.ReconciliationCoverage)
	}

	return nil
}

// CompleteReconciliations returns the sum of all failed, exempt, and successful
// reconciliations.
func (t *DataTester) CompleteReconciliations(ctx context.Context) (int64, error) {
	activeReconciliations, err := t.counterStorage.Get(ctx, modules.ActiveReconciliationCounter)
	if err != nil {
		return -1, fmt.Errorf("failed to get active reconciliations counter: %w", err)
	}

	exemptReconciliations, err := t.counterStorage.Get(ctx, modules.ExemptReconciliationCounter)
	if err != nil {
		return -1, fmt.Errorf("failed to get exempt reconciliations counter: %w", err)
	}

	failedReconciliations, err := t.counterStorage.Get(ctx, modules.FailedReconciliationCounter)
	if err != nil {
		return -1, fmt.Errorf("failed to get failed reconciliations counter: %w", err)
	}

	skippedReconciliations, err := t.counterStorage.Get(ctx, modules.SkippedReconciliationsCounter)
	if err != nil {
		return -1, fmt.Errorf("failed to get skipped reconciliations counter: %w", err)
	}

	return activeReconciliations.Int64() +
		exemptReconciliations.Int64() +
		failedReconciliations.Int64() +
		skippedReconciliations.Int64(), nil
}

// WaitForEmptyQueue exits once the active reconciler
// queue is empty and all reconciler goroutines are idle.
func (t *DataTester) WaitForEmptyQueue(
	ctx context.Context,
) error {
	// To ensure we don't exit while a reconciliation is ongoing
	// (i.e. when queue size is 0 but there are busy threads),
	// we keep track of how many reconciliations we must complete
	// and only exit when that many reconciliations have been performed.
	startingComplete, err := t.CompleteReconciliations(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete reconciliations: %w", err)
	}
	startingRemaining := t.reconciler.QueueSize()

	tc := time.NewTicker(EndAtTipCheckInterval)
	defer tc.Stop()

	color.Cyan(
		"[PROGRESS] remaining reconciliations: %d",
		startingRemaining,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-tc.C:
			// We force cached counts to be written before
			// determining if we should exit.
			if err := t.reconcilerHandler.UpdateCounts(ctx); err != nil {
				return fmt.Errorf("failed to update count: %w", err)
			}

			nowComplete, err := t.CompleteReconciliations(ctx)
			if err != nil {
				return fmt.Errorf("failed to complete reconciliations: %w", err)
			}

			completed := nowComplete - startingComplete
			remaining := int64(startingRemaining) - completed
			if remaining <= 0 {
				t.cancel()
				return nil
			}

			color.Cyan(
				"[PROGRESS] remaining reconciliations: %d",
				remaining,
			)
		}
	}
}

// DrainReconcilerQueue returns once the reconciler queue has been drained
// or an error is encountered.
func (t *DataTester) DrainReconcilerQueue(
	ctx context.Context,
	sigListeners *[]context.CancelFunc,
) error {
	color.Cyan("draining reconciler backlog (you can disable this in your configuration file)")

	// To cancel all execution, need to call multiple cancel functions.
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	*sigListeners = append(*sigListeners, cancel)

	// Disable inactive lookups
	t.reconciler.InactiveConcurrency = 0

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return t.StartReconciler(ctx)
	})
	g.Go(func() error {
		return t.WaitForEmptyQueue(ctx)
	})

	err := g.Wait()

	if *t.signalReceived {
		return cliErrs.ErrReconcilerDrainHalt
	}

	if errors.Is(err, context.Canceled) {
		color.Cyan("drained reconciler backlog")
		return nil
	}

	return err
}

// HandleErr is called when `check:data` returns an error.
// If historical balance lookups are enabled, HandleErr will attempt to
// automatically find any missing balance-changing operations.
func (t *DataTester) HandleErr(err error, sigListeners *[]context.CancelFunc) error {
	// Initialize new context because calling context
	// will no longer be usable when after termination.
	ctx := context.Background()

	if *t.signalReceived {
		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			fmt.Errorf("%v: %w", err.Error(), cliErrs.ErrDataCheckHalt),
			"",
			"",
		)
	}

	if (err == nil || errors.Is(err, context.Canceled)) &&
		len(t.endCondition) == 0 && t.config.Data.EndConditions != nil &&
		t.config.Data.EndConditions.Index != nil { // occurs at syncer end
		t.endCondition = configuration.IndexEndCondition
		t.endConditionDetail = fmt.Sprintf(
			"Index: %d",
			*t.config.Data.EndConditions.Index,
		)
	}

	// End condition will only be populated if there is
	// no error.
	if len(t.endCondition) != 0 {
		// Wait for reconciliation queue to drain (only if end condition reached)
		if shouldReconcile(t.config) &&
			t.reconciler.QueueSize() > 0 {
			if t.config.Data.ReconciliationDrainDisabled {
				color.Cyan(
					"skipping reconciler backlog drain (you can enable this in your configuration file)",
				)
			} else {
				drainErr := t.DrainReconcilerQueue(ctx, sigListeners)
				if drainErr != nil {
					return results.ExitData(
						t.config,
						t.counterStorage,
						t.balanceStorage,
						drainErr,
						"",
						"",
					)
				}
			}
		}

		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			nil,
			t.endCondition,
			t.endConditionDetail,
		)
	}

	fmt.Printf("\n")
	if t.reconcilerHandler.InactiveFailure == nil {
		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			err,
			"",
			"",
		)
	}

	if !t.historicalBalanceEnabled {
		color.Yellow(
			"Can't find the block missing operations automatically, please enable historical balance lookup",
		)
		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			err,
			"",
			"",
		)
	}

	if t.config.Data.InactiveDiscrepancySearchDisabled {
		color.Yellow("Search for inactive reconciliation discrepancy is disabled")
		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			err,
			"",
			"",
		)
	}

	return t.FindMissingOps(ctx, err, sigListeners)
}

// FindMissingOps logs the types.BlockIdentifier of a block
// that is missing balance-changing operations for a
// *types.AccountCurrency.
func (t *DataTester) FindMissingOps(
	ctx context.Context,
	originalErr error,
	sigListeners *[]context.CancelFunc,
) error {
	color.Cyan("Searching for block with missing operations...hold tight")
	badBlock, err := t.recursiveOpSearch(
		ctx,
		sigListeners,
		t.reconcilerHandler.InactiveFailure,
		t.reconcilerHandler.InactiveFailureBlock.Index-InactiveFailureLookbackWindow,
		t.reconcilerHandler.InactiveFailureBlock.Index,
	)
	if err != nil {
		color.Yellow("could not find block with missing ops: %s", err.Error())
		return results.ExitData(
			t.config,
			t.counterStorage,
			t.balanceStorage,
			originalErr,
			"",
			"",
		)
	}

	color.Yellow(
		"Missing ops for %s in block %d:%s",
		types.AccountString(t.reconcilerHandler.InactiveFailure.Account),
		badBlock.Index,
		badBlock.Hash,
	)

	return results.ExitData(
		t.config,
		t.counterStorage,
		t.balanceStorage,
		originalErr,
		"",
		"",
	)
}

func (t *DataTester) recursiveOpSearch(
	ctx context.Context,
	sigListeners *[]context.CancelFunc,
	accountCurrency *types.AccountCurrency,
	startIndex int64,
	endIndex int64,
) (*types.BlockIdentifier, error) {
	// To cancel all execution, need to call multiple cancel functions.
	ctx, cancel := context.WithCancel(ctx)
	*sigListeners = append(*sigListeners, cancel)

	// Always use a temporary directory to find missing ops
	tmpDir, err := utils.CreateTempDir()
	if err != nil {
		return nil, fmt.Errorf("unable to create temporary directory: %w", err)
	}
	defer utils.RemoveTempDir(tmpDir)

	localStore, err := database.NewBadgerDatabase(ctx, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize database: %w", err)
	}

	counterStorage := modules.NewCounterStorage(localStore)
	blockStorage := modules.NewBlockStorage(localStore, t.config.SerialBlockWorkers)
	balanceStorage := modules.NewBalanceStorage(localStore)

	logger, err := logger.NewLogger(
		tmpDir,
		false,
		false,
		false,
		false,
		logger.Data,
		t.network,
	)

	if err != nil {
		return nil, fmt.Errorf("unable to initialize logger with error: %w", err)
	}

	t.forceInactiveReconciliation = types.Bool(false)
	reconcilerHelper := processor.NewReconcilerHelper(
		t.config,
		t.network,
		t.fetcher,
		localStore,
		blockStorage,
		balanceStorage,
		t.forceInactiveReconciliation,
	)

	reconcilerHandler := processor.NewReconcilerHandler(
		logger,
		counterStorage,
		balanceStorage,
		true, // halt on reconciliation error
	)

	r := reconciler.New(
		reconcilerHelper,
		reconcilerHandler,
		t.parser,

		// When using concurrency > 1, we could start looking up balance changes
		// on multiple blocks at once. This can cause us to return the wrong block
		// that is missing operations.
		reconciler.WithActiveConcurrency(1),

		// Do not do any inactive lookups when looking for the block with missing
		// operations.
		reconciler.WithInactiveConcurrency(0),
		reconciler.WithLookupBalanceByBlock(),
		reconciler.WithInterestingAccounts([]*types.AccountCurrency{accountCurrency}),
	)

	balanceStorageHelper := processor.NewBalanceStorageHelper(
		t.network,
		t.fetcher,
		counterStorage,
		t.historicalBalanceEnabled,
		nil,
		false,
		t.parser.BalanceExemptions,
		false, // we will need to perform an initial balance fetch when finding issues
	)

	balanceStorageHandler := processor.NewBalanceStorageHandler(
		logger,
		r,
		counterStorage,
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
		[]modules.BlockWorker{balanceStorage},
		statefulsyncer.WithCacheSize(syncer.DefaultCacheSize),
		statefulsyncer.WithMaxConcurrency(t.config.MaxSyncConcurrency),
		statefulsyncer.WithPastBlockLimit(t.config.MaxReorgDepth),
		statefulsyncer.WithSeenConcurrency(int64(t.config.SeenBlockWorkers)),
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
		return nil, fmt.Errorf("unable to close database: %w", storageErr)
	}

	if *t.signalReceived {
		return nil, cliErrs.ErrMissingOps
	}

	if err == nil || errors.Is(err, context.Canceled) {
		if startIndex <= t.genesisBlock.Index {
			return nil, cliErrs.ErrUnableToFindMissingOps
		}

		newStart := startIndex - InactiveFailureLookbackWindow
		if newStart < t.genesisBlock.Index {
			newStart = t.genesisBlock.Index
		}

		newEnd := endIndex - InactiveFailureLookbackWindow
		if newEnd <= newStart {
			return nil, fmt.Errorf(
				"next window to check has start index %d <= end index %d",
				newStart,
				newEnd,
			)
		}

		color.Cyan(
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
		return nil, cliErrs.ErrUnableToFindMissingOps
	}

	return reconcilerHandler.ActiveFailureBlock, nil
}
