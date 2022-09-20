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
	"net/http"
	"time"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/results"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
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
	// constructionCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	constructionCmdName = "check-construction"

	endConditionsCheckInterval = 10 * time.Second
	tipWaitInterval            = 10 * time.Second
)

var _ http.Handler = (*ConstructionTester)(nil)

// ConstructionTester coordinates the `check:construction` test.
type ConstructionTester struct {
	network          *types.NetworkIdentifier
	database         database.Database
	config           *configuration.Configuration
	syncer           *statefulsyncer.StatefulSyncer
	logger           *logger.Logger
	onlineFetcher    *fetcher.Fetcher
	broadcastStorage *modules.BroadcastStorage
	blockStorage     *modules.BlockStorage
	jobStorage       *modules.JobStorage
	counterStorage   *modules.CounterStorage
	coordinator      *coordinator.Coordinator
	cancel           context.CancelFunc
	signalReceived   *bool

	reachedEndConditions bool
}

// InitializeConstruction initiates the construction API tester.
func InitializeConstruction(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	onlineFetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
	signalReceived *bool,
) (*ConstructionTester, error) {
	dataPath, err := utils.CreateCommandPath(config.DataDirectory, constructionCmdName, network)
	if err != nil {
		return nil, fmt.Errorf("failed to create command path: %w", err)
	}

	opts := []database.BadgerOption{}
	if config.CompressionDisabled {
		opts = append(opts, database.WithoutCompression())
	}
	if config.L0InMemoryEnabled {
		opts = append(
			opts,
			database.WithCustomSettings(database.PerformanceBadgerOptions(dataPath)),
		)
	}

	localStore, err := database.NewBadgerDatabase(ctx, dataPath, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize database: %w", err)
	}

	networkOptions, fetchErr := onlineFetcher.NetworkOptionsRetry(ctx, network, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get network options: %w", fetchErr.Err)
	}

	if len(networkOptions.Allow.BalanceExemptions) > 0 &&
		config.Construction.InitialBalanceFetchDisabled {
		return nil, cliErrs.ErrBalanceExemptionsWithInitialBalanceFetchDisabled
	}

	counterStorage := modules.NewCounterStorage(localStore)
	logger, err := logger.NewLogger(
		dataPath,
		false,
		false,
		false,
		false,
		logger.Construction,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize logger with error: %w", err)
	}

	blockStorage := modules.NewBlockStorage(localStore, config.SerialBlockWorkers)
	keyStorage := modules.NewKeyStorage(localStore)
	coinStorageHelper := processor.NewCoinStorageHelper(blockStorage)
	coinStorage := modules.NewCoinStorage(localStore, coinStorageHelper, onlineFetcher.Asserter)
	balanceStorage := modules.NewBalanceStorage(localStore)

	balanceStorageHelper := processor.NewBalanceStorageHelper(
		network,
		onlineFetcher,
		counterStorage,
		false,
		nil,
		true,
		networkOptions.Allow.BalanceExemptions,
		config.Construction.InitialBalanceFetchDisabled,
	)

	balanceStorageHandler := processor.NewBalanceStorageHandler(
		logger,
		nil,
		counterStorage,
		false,
		nil,
	)

	balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

	broadcastStorage := modules.NewBroadcastStorage(
		localStore,
		config.Construction.StaleDepth,
		config.Construction.BroadcastLimit,
		config.TipDelay,
		config.Construction.BroadcastBehindTip,
		config.Construction.BlockBroadcastLimit,
	)

	parser := parser.New(onlineFetcher.Asserter, nil, networkOptions.Allow.BalanceExemptions)
	broadcastHelper := processor.NewBroadcastStorageHelper(
		network,
		blockStorage,
		onlineFetcher,
	)

	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(config.Construction.MaxOfflineConnections),
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	}
	if config.Construction.ForceRetry {
		fetcherOpts = append(fetcherOpts, fetcher.WithForceRetry())
	}

	offlineFetcher := fetcher.New(
		config.Construction.OfflineURL,
		fetcherOpts...,
	)

	// Import prefunded account and save to database
	err = keyStorage.ImportAccounts(ctx, config.Construction.PrefundedAccounts)
	if err != nil {
		return nil, err
	}

	// Load all accounts for network
	accounts, err := keyStorage.GetAllAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load addresses: %w", err)
	}

	// Track balances on all addresses
	for _, account := range accounts {
		balanceStorageHelper.AddInterestingAddress(account.Address)
	}

	log.Printf("construction tester initialized with %d accounts\n", len(accounts))

	// Load prefunded accounts
	var accountBalanceRequests []*utils.AccountBalanceRequest
	var acctCoinsReqs []*utils.AccountCoinsRequest
	for _, prefundedAcc := range config.Construction.PrefundedAccounts {
		accountBalance := &utils.AccountBalanceRequest{
			Account:  prefundedAcc.AccountIdentifier,
			Network:  network,
			Currency: prefundedAcc.Currency,
		}

		accountBalanceRequests = append(accountBalanceRequests, accountBalance)

		if config.CoinSupported {
			acctCoinsReq := &utils.AccountCoinsRequest{
				Account:        prefundedAcc.AccountIdentifier,
				Network:        network,
				Currencies:     []*types.Currency{prefundedAcc.Currency},
				IncludeMempool: false,
			}

			acctCoinsReqs = append(acctCoinsReqs, acctCoinsReq)
		}
	}

	accBalances, err := utils.GetAccountBalances(ctx, onlineFetcher, accountBalanceRequests)
	if err != nil {
		return nil, fmt.Errorf("unable to get account balances: %w", err)
	}

	err = balanceStorage.SetBalanceImported(ctx, nil, accBalances)
	if err != nil {
		return nil, fmt.Errorf("unable to set balances: %w", err)
	}

	// -------------------------------------------------------------------------
	// ------------ Get account coins and add them in coins storage ------------
	// -------------------------------------------------------------------------

	if config.CoinSupported {
		acctCoins, errAccCoins := utils.GetAccountCoins(ctx, onlineFetcher, acctCoinsReqs)
		if errAccCoins != nil {
			return nil, fmt.Errorf("unable to get account coins: %w", errAccCoins)
		}

		// Extract accounts from account coins requests
		var accts []*types.AccountIdentifier
		for _, req := range acctCoinsReqs {
			accts = append(accts, req.Account)
		}

		err = coinStorage.SetCoinsImported(ctx, accts, acctCoins)
		if err != nil {
			return nil, fmt.Errorf("unable to set coin balances: %w", err)
		}
	}

	// --------------------------------------------------------------------------
	// ---------------------- End of adding account coins -----------------------
	// --------------------------------------------------------------------------

	jobStorage := modules.NewJobStorage(localStore)
	coordinatorHelper := processor.NewCoordinatorHelper(
		offlineFetcher,
		onlineFetcher,
		localStore,
		blockStorage,
		keyStorage,
		balanceStorage,
		coinStorage,
		broadcastStorage,
		balanceStorageHelper,
		counterStorage,
		config.Construction.Quiet,
	)

	coordinatorHandler := processor.NewCoordinatorHandler(
		counterStorage,
	)
	coordinator, err := coordinator.New(
		jobStorage,
		coordinatorHelper,
		coordinatorHandler,
		parser,
		config.Construction.Workflows,
	)
	if err != nil {
		log.Fatalf("unable to create coordinator: %s", err.Error())
	}

	broadcastHandler := processor.NewBroadcastStorageHandler(
		config,
		blockStorage,
		counterStorage,
		coordinator,
		parser,
	)

	broadcastStorage.Initialize(broadcastHelper, broadcastHandler)

	syncer := statefulsyncer.New(
		ctx,
		network,
		onlineFetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]modules.BlockWorker{counterStorage, balanceStorage, coinStorage, broadcastStorage},
		statefulsyncer.WithCacheSize(syncer.DefaultCacheSize),
		statefulsyncer.WithMaxConcurrency(config.MaxSyncConcurrency),
		statefulsyncer.WithPastBlockLimit(config.MaxReorgDepth),
		statefulsyncer.WithSeenConcurrency(int64(config.SeenBlockWorkers)),
	)

	return &ConstructionTester{
		network:          network,
		database:         localStore,
		config:           config,
		syncer:           syncer,
		logger:           logger,
		coordinator:      coordinator,
		broadcastStorage: broadcastStorage,
		blockStorage:     blockStorage,
		jobStorage:       jobStorage,
		counterStorage:   counterStorage,
		onlineFetcher:    onlineFetcher,
		cancel:           cancel,
		signalReceived:   signalReceived,
	}, nil
}

// CloseDatabase closes the database used by ConstructionTester.
func (t *ConstructionTester) CloseDatabase(ctx context.Context) {
	if err := t.database.Close(ctx); err != nil {
		log.Fatalf("error closing database: %s", err.Error())
	}
}

// StartPeriodicLogger prints out periodic
// stats about a run of `check:construction`.
func (t *ConstructionTester) StartPeriodicLogger(
	ctx context.Context,
) error {
	tc := time.NewTicker(PeriodicLoggingFrequency)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tc.C:
			status := results.ComputeCheckConstructionStatus(
				ctx,
				t.config,
				t.counterStorage,
				t.broadcastStorage,
				t.jobStorage,
			)
			t.logger.LogConstructionStatus(ctx, status)
		}
	}
}

func (t *ConstructionTester) checkTip(ctx context.Context) (int64, error) {
	atTip, blockIdentifier, err := utils.CheckNetworkTip(
		ctx,
		t.network,
		t.config.TipDelay,
		t.onlineFetcher,
	)
	if err != nil {
		return -1, fmt.Errorf("failed to check network tip: %w", err)
	}

	if atTip {
		return blockIdentifier.Index, nil
	}

	return -1, nil
}

// waitForTip loops until the Rosetta implementation is at tip.
func (t *ConstructionTester) waitForTip(ctx context.Context) (int64, error) {
	tc := time.NewTicker(tipWaitInterval)
	defer tc.Stop()

	for {
		// Don't wait any time before first tick if at tip.
		tipIndex, err := t.checkTip(ctx)
		if err != nil {
			return -1, fmt.Errorf("failed to check tip: %w", err)
		}

		if tipIndex != -1 {
			return tipIndex, nil
		}

		log.Println("waiting for implementation to reach tip before testing...")

		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-tc.C:
			continue
		}
	}
}

// StartSyncer uses the tester's stateful syncer
// to compute balance changes and track transactions
// for confirmation on-chain.
func (t *ConstructionTester) StartSyncer(
	ctx context.Context,
	cancel context.CancelFunc,
) error {
	startIndex := int64(-1)
	_, err := t.blockStorage.GetHeadBlockIdentifier(ctx)
	if errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
		// If no head block exists, ensure we are at tip before starting. Otherwise,
		// we will unnecessarily sync tons of blocks before reaching any that matter.
		startIndex, err = t.waitForTip(ctx)
		if err != nil {
			return fmt.Errorf("unable to wait for tip: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("unable to get last block synced: %w", err)
	}

	return t.syncer.Sync(ctx, startIndex, -1)
}

// StartConstructor uses the tester's constructor
// to begin generating addresses and constructing
// transactions.
func (t *ConstructionTester) StartConstructor(
	ctx context.Context,
) error {
	if t.config.Construction.ClearBroadcasts {
		broadcasts, err := t.broadcastStorage.ClearBroadcasts(ctx)
		if err != nil {
			return fmt.Errorf("unable to clear broadcasts: %w", err)
		}

		log.Printf("cleared %d broadcasts\n", len(broadcasts))
	}

	return t.coordinator.Process(ctx)
}

// ServeHTTP serves a CheckDataStatus response on all paths.
func (t *ConstructionTester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	status := results.ComputeCheckConstructionStatus(
		r.Context(),
		t.config,
		t.counterStorage,
		t.broadcastStorage,
		t.jobStorage,
	)

	if err := json.NewEncoder(w).Encode(status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// PerformBroadcasts attempts to rebroadcast all pending transactions
// if the RebroadcastAll configuration is set to true.
func (t *ConstructionTester) PerformBroadcasts(ctx context.Context) error {
	if !t.config.Construction.RebroadcastAll {
		return nil
	}

	color.Magenta("Rebroadcasting all transactions...")

	if err := t.broadcastStorage.BroadcastAll(ctx, false); err != nil {
		return fmt.Errorf("unable to broadcast all transactions: %w", err)
	}

	return nil
}

// WatchEndConditions cancels check:construction once
// all end conditions are met (provided workflows
// are executed at least minOccurences).
func (t *ConstructionTester) WatchEndConditions(
	ctx context.Context,
) error {
	endConditions := t.config.Construction.EndConditions
	if endConditions == nil {
		return nil
	}

	tc := time.NewTicker(endConditionsCheckInterval)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tc.C:
			conditionsMet := true
			for workflow, minOccurences := range endConditions {
				completed, err := t.jobStorage.Completed(ctx, workflow)
				if err != nil {
					return fmt.Errorf("unable to fetch completed %s: %w", workflow, err)
				}

				if len(completed) < minOccurences {
					conditionsMet = false
					break
				}
			}

			if conditionsMet {
				t.reachedEndConditions = true
				t.cancel()
				return nil
			}
		}
	}
}

func (t *ConstructionTester) returnFunds(
	ctx context.Context,
	sigListeners *[]context.CancelFunc,
) {
	// To cancel all execution, need to call multiple cancel functions.
	ctx, cancel := context.WithCancel(ctx)
	*sigListeners = append(*sigListeners, cancel)

	var returnFundsSuccess bool
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return t.StartSyncer(ctx, cancel)
	})
	g.Go(func() error {
		return t.StartPeriodicLogger(ctx)
	})
	g.Go(func() error {
		err := t.coordinator.ReturnFunds(ctx)

		// If the error is nil, we need to cancel the syncer
		// or we will sync forever.
		if err == nil {
			returnFundsSuccess = true // makes error parsing much easier
			cancel()
			return nil
		}

		return err
	})

	err := g.Wait()
	if *t.signalReceived {
		color.Red("Fund return halted")
		return
	}

	if !returnFundsSuccess {
		log.Printf("unable to return funds %v\n", err)
	}
}

// HandleErr is called when `check:construction` returns an error.
func (t *ConstructionTester) HandleErr(
	err error,
	sigListeners *[]context.CancelFunc,
) error {
	if *t.signalReceived {
		return results.ExitConstruction(
			t.config,
			t.counterStorage,
			t.jobStorage,
			fmt.Errorf("%v: %w", err.Error(), cliErrs.ErrConstructionCheckHalt),
		)
	}

	if !t.reachedEndConditions {
		return results.ExitConstruction(t.config, t.counterStorage, t.jobStorage, err)
	}

	// We optimistically run the ReturnFunds function on the coordinator
	// and only log if it fails. If there is no ReturnFunds workflow defined,
	// this will just return nil.
	t.returnFunds(
		context.Background(),
		sigListeners,
	)

	return results.ExitConstruction(t.config, t.counterStorage, t.jobStorage, nil)
}
