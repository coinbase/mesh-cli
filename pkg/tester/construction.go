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
	"os"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/processor"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage"
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

// ConstructionTester coordinates the `check:construction` test.
type ConstructionTester struct {
	network          *types.NetworkIdentifier
	database         storage.Database
	config           *configuration.Configuration
	syncer           *statefulsyncer.StatefulSyncer
	logger           *logger.Logger
	onlineFetcher    *fetcher.Fetcher
	broadcastStorage *storage.BroadcastStorage
	blockStorage     *storage.BlockStorage
	jobStorage       *storage.JobStorage
	counterStorage   *storage.CounterStorage
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
		log.Fatalf("%s: cannot create command path", err.Error())
	}

	localStore, err := storage.NewBadgerStorage(ctx, dataPath)
	if err != nil {
		log.Fatalf("%s: unable to initialize database", err.Error())
	}

	counterStorage := storage.NewCounterStorage(localStore)
	logger := logger.NewLogger(
		counterStorage,
		nil,
		dataPath,
		false,
		false,
		false,
		false,
	)

	blockStorage := storage.NewBlockStorage(localStore)
	keyStorage := storage.NewKeyStorage(localStore)
	coinStorageHelper := processor.NewCoinStorageHelper(blockStorage)
	coinStorage := storage.NewCoinStorage(localStore, coinStorageHelper, onlineFetcher.Asserter)
	balanceStorage := storage.NewBalanceStorage(localStore)

	balanceStorageHelper := processor.NewBalanceStorageHelper(
		network,
		onlineFetcher,
		false,
		nil,
		true,
	)

	balanceStorageHandler := processor.NewBalanceStorageHandler(
		logger,
		nil,
		false,
		nil,
	)

	balanceStorage.Initialize(balanceStorageHelper, balanceStorageHandler)

	broadcastStorage := storage.NewBroadcastStorage(
		localStore,
		config.Construction.StaleDepth,
		config.Construction.BroadcastLimit,
		config.TipDelay,
		config.Construction.BroadcastBehindTip,
		config.Construction.BlockBroadcastLimit,
	)

	parser := parser.New(onlineFetcher.Asserter, nil)
	broadcastHelper := processor.NewBroadcastStorageHelper(
		blockStorage,
		onlineFetcher,
	)
	offlineFetcher := fetcher.New(
		config.Construction.OfflineURL,
		fetcher.WithMaxConnections(config.Construction.MaxOfflineConnections),
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	)

	// Import prefunded account and save to database
	err = keyStorage.ImportAccounts(ctx, config.Construction.PrefundedAccounts)
	if err != nil {
		return nil, err
	}

	// Load all accounts for network
	accounts, err := keyStorage.GetAllAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to load addresses", err)
	}

	// Track balances on all addresses
	for _, account := range accounts {
		balanceStorageHelper.AddInterestingAddress(account.Address)
	}

	log.Printf("construction tester initialized with %d accounts\n", len(accounts))

	// Load prefunded accounts
	var accountBalanceRequests []*utils.AccountBalanceRequest
	for _, prefundedAcc := range config.Construction.PrefundedAccounts {
		accountBalance := &utils.AccountBalanceRequest{
			Account:  prefundedAcc.AccountIdentifier,
			Network:  network,
			Currency: prefundedAcc.Currency,
		}

		accountBalanceRequests = append(accountBalanceRequests, accountBalance)
	}

	accBalances, err := utils.GetAccountBalances(ctx, onlineFetcher, accountBalanceRequests)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get account balances", err)
	}

	err = balanceStorage.SetBalanceImported(ctx, nil, accBalances)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to set balances", err)
	}

	err = coinStorage.SetCoinsImported(ctx, accBalances)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to set coin balances", err)
	}

	jobStorage := storage.NewJobStorage(localStore)
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
		log.Fatalf("%s: unable to create coordinator", err.Error())
	}

	broadcastHandler := processor.NewBroadcastStorageHandler(
		config,
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
		[]storage.BlockWorker{balanceStorage, coinStorage, broadcastStorage},
		syncer.DefaultCacheSize,
		config.MaxSyncConcurrency,
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
		log.Fatalf("%s: error closing database", err.Error())
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
			// Print stats one last time before exiting
			inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
			_ = t.logger.LogConstructionStats(ctx, len(inflight))

			return ctx.Err()
		case <-tc.C:
			inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
			_ = t.logger.LogConstructionStats(ctx, len(inflight))
		}
	}
}

func (t *ConstructionTester) checkTip(ctx context.Context) (int64, error) {
	status, fetchErr := t.onlineFetcher.NetworkStatusRetry(ctx, t.network, nil)
	if fetchErr != nil {
		return -1, fmt.Errorf("%w: unable to fetch network status", fetchErr.Err)
	}

	// If a block has yet to be synced, start syncing from tip.
	if utils.AtTip(t.config.TipDelay, status.CurrentBlockTimestamp) {
		return status.CurrentBlockIdentifier.Index, nil
	}

	return -1, nil
}

// waitForTip loops until the Rosetta implementation is at tip.
func (t *ConstructionTester) waitForTip(ctx context.Context) (int64, error) {
	// Don't wait any time before first tick if at tip.
	tipIndex, err := t.checkTip(ctx)
	if err != nil {
		return -1, err
	}
	if tipIndex != -1 {
		return tipIndex, nil
	}

	tc := time.NewTicker(tipWaitInterval)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-tc.C:
			tipIndex, err := t.checkTip(ctx)
			if err != nil {
				return -1, err
			}

			if tipIndex != -1 {
				return tipIndex, nil
			}

			log.Println("waiting for implementation to reach tip before testing...")
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
	if errors.Is(err, storage.ErrHeadBlockNotFound) {
		// If no head block exists, ensure we are at tip before starting. Otherwise,
		// we will unnecessarily sync tons of blocks before reaching any that matter.
		startIndex, err = t.waitForTip(ctx)
		if err != nil {
			return fmt.Errorf("%w: unable to wait for tip", err)
		}
	} else if err != nil {
		return fmt.Errorf("%w: unable to get last block synced", err)
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
			return fmt.Errorf("%w: unable to clear broadcasts", err)
		}

		log.Printf("cleared %d broadcasts\n", len(broadcasts))
	}

	return t.coordinator.Process(ctx)
}

// PerformBroadcasts attempts to rebroadcast all pending transactions
// if the RebroadcastAll configuration is set to true.
func (t *ConstructionTester) PerformBroadcasts(ctx context.Context) error {
	if !t.config.Construction.RebroadcastAll {
		return nil
	}

	color.Magenta("Rebroadcasting all transactions...")

	if err := t.broadcastStorage.BroadcastAll(ctx, false); err != nil {
		return fmt.Errorf("%w: unable to broadcast all transactions", err)
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
					return fmt.Errorf("%w: unable to fetch completed %s", err, workflow)
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
) {
	if *t.signalReceived {
		color.Red("Check halted")
		os.Exit(1)
		return
	}

	if !t.reachedEndConditions {
		ExitConstruction(t.config, t.counterStorage, t.jobStorage, err, 1)
	}

	// We optimistically run the ReturnFunds function on the coordinator
	// and only log if it fails. If there is no ReturnFunds workflow defined,
	// this will just return nil.
	t.returnFunds(
		context.Background(),
		sigListeners,
	)

	ExitConstruction(t.config, t.counterStorage, t.jobStorage, nil, 0)
}
