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
var constructionMetadata string

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
	metadataMap := logger.ConvertStringToMap(config.InfoMetaData)
	metadataMap = logger.AddRequestUUIDToMap(metadataMap, config.RequestUUID)
	constructionMetadata = logger.ConvertMapToString(metadataMap)
	if err != nil {
		err = fmt.Errorf("failed to create command path: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	opts := []database.BadgerOption{}
	// add constructionMetadata into localStore
	opts = append(opts, database.WithMetaData(constructionMetadata))
	if config.CompressionDisabled {
		opts = append(opts, database.WithoutCompression())
	}
	if config.L0InMemoryEnabled {
		opts = append(
			opts,
			database.WithCustomSettings(database.PerformanceBadgerOptions(dataPath)),
		)
	}

	// add constructionMetadata into localStore
	localStore, err := database.NewBadgerDatabase(ctx, dataPath, opts...)
	if err != nil {
		err = fmt.Errorf("unable to initialize database: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	networkOptions, fetchErr := onlineFetcher.NetworkOptionsRetry(ctx, network, nil)
	if fetchErr != nil {
		err := fmt.Errorf("unable to get network options: %w%s", fetchErr.Err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	if len(networkOptions.Allow.BalanceExemptions) > 0 &&
		config.Construction.InitialBalanceFetchDisabled {
		return nil, cliErrs.ErrBalanceExemptionsWithInitialBalanceFetchDisabled
	}

	counterStorage := modules.NewCounterStorage(localStore)
	//add constructionMetadata into logger
	logger, err := logger.NewLogger(
		dataPath,
		false,
		false,
		false,
		false,
		logger.Construction,
		network,
		metadataMap,
	)
	if err != nil {
		err = fmt.Errorf("unable to initialize logger with error: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
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
	//add constructionMetadata into fetcher
	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(config.Construction.MaxOfflineConnections),
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
		fetcher.WithMetaData(constructionMetadata),
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
		err = fmt.Errorf("%w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	// Load all accounts for network
	accounts, err := keyStorage.GetAllAccounts(ctx)
	if err != nil {
		err = fmt.Errorf("unable to load addresses: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	// Track balances on all addresses
	for _, account := range accounts {
		balanceStorageHelper.AddInterestingAddress(account.Address)
	}

	color.Cyan("construction tester initialized with %d accounts%s\n", len(accounts), constructionMetadata)

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
		err = fmt.Errorf("unable to get account balances: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	err = balanceStorage.SetBalanceImported(ctx, nil, accBalances)
	if err != nil {
		err = fmt.Errorf("unable to set balances: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return nil, err
	}

	// -------------------------------------------------------------------------
	// ------------ Get account coins and add them in coins storage ------------
	// -------------------------------------------------------------------------

	if config.CoinSupported {
		acctCoins, errAccCoins := utils.GetAccountCoins(ctx, onlineFetcher, acctCoinsReqs)
		if errAccCoins != nil {
			err = fmt.Errorf("unable to get account coins: %w%s", errAccCoins, constructionMetadata)
			color.Red(err.Error())
			return nil, err
		}

		// Extract accounts from account coins requests
		var accts []*types.AccountIdentifier
		for _, req := range acctCoinsReqs {
			accts = append(accts, req.Account)
		}

		err = coinStorage.SetCoinsImported(ctx, accts, acctCoins)
		if err != nil {
			err = fmt.Errorf("unable to set coin balances: %w%s", err, constructionMetadata)
			color.Red(err.Error())
			return nil, err
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
		msg := fmt.Sprintf("unable to create coordinator: %s%s", err.Error(), constructionMetadata)
		color.Red(msg)
		log.Fatalf(msg)
	}

	broadcastHandler := processor.NewBroadcastStorageHandler(
		config,
		blockStorage,
		counterStorage,
		coordinator,
		parser,
	)

	broadcastStorage.Initialize(broadcastHelper, broadcastHandler)

	//add constructionMetadata into syncer
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
		statefulsyncer.WithMetaData(constructionMetadata),
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
		msg := fmt.Sprintf("error closing database: %s%s", err.Error(), constructionMetadata)
		color.Red(msg)
		log.Fatalf(msg)
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
		err = fmt.Errorf("failed to check network tip: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return -1, err
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
			err = fmt.Errorf("failed to check tip: %w%s", err, constructionMetadata)
			color.Red(err.Error())
			return -1, err
		}

		if tipIndex != -1 {
			return tipIndex, nil
		}

		color.Cyan("waiting for implementation to reach tip before testing...%s", constructionMetadata)

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
			err = fmt.Errorf("unable to wait for tip: %w%s", err, constructionMetadata)
			color.Red(err.Error())
			return err
		}
	} else if err != nil {
		err = fmt.Errorf("unable to get last block synced: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return err
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
			err = fmt.Errorf("unable to clear broadcasts: %w%s", err, constructionMetadata)
			color.Red(err.Error())
			return err
		}

		color.Cyan("cleared %d broadcasts%s\n", len(broadcasts), constructionMetadata)
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

	color.Magenta("Rebroadcasting all transactions...%s", constructionMetadata)

	if err := t.broadcastStorage.BroadcastAll(ctx, false); err != nil {
		err = fmt.Errorf("unable to broadcast all transactions: %w%s", err, constructionMetadata)
		color.Red(err.Error())
		return err
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
					err = fmt.Errorf("unable to fetch completed %s: %w%s", workflow, err, constructionMetadata)
					color.Red(err.Error())
					return err
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
		color.Red("Fund return halted%s", constructionMetadata)
		return
	}

	if !returnFundsSuccess {
		color.Cyan("unable to return funds %v%s\n", err, constructionMetadata)
	}
}

// HandleErr is called when `check:construction` returns an error.
func (t *ConstructionTester) HandleErr(
	err error,
	sigListeners *[]context.CancelFunc,
) error {
	if *t.signalReceived {
		err = fmt.Errorf("%v: %w%s", err.Error(), cliErrs.ErrConstructionCheckHalt, constructionMetadata)
		color.Red(err.Error())
		return results.ExitConstruction(
			t.config,
			t.counterStorage,
			t.jobStorage,
			err,
		)
	}

	if !t.reachedEndConditions {
		color.Red("%v%s", err, constructionMetadata)
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
