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
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/processor"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
)

const (
	// constructionCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	constructionCmdName = "check-construction"
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
	coordinator      *coordinator.Coordinator
}

// InitializeConstruction initiates the construction API tester.
func InitializeConstruction(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	onlineFetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
) (*ConstructionTester, error) {
	if len(config.Construction.Workflows) == 0 {
		log.Fatal("construction workflows cannot be empty")
	}

	dataPath, err := utils.CreateCommandPath(config.DataDirectory, constructionCmdName, network)
	if err != nil {
		log.Fatalf("%s: cannot create command path", err.Error())
	}

	opts := []storage.BadgerOption{}
	if !config.DisableMemoryLimit {
		opts = append(opts, storage.WithMemoryLimit())
	}
	localStore, err := storage.NewBadgerStorage(ctx, dataPath, opts...)
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
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
	)

	// Import prefunded account and save to database
	err = keyStorage.ImportAccounts(ctx, config.Construction.PrefundedAccounts)
	if err != nil {
		return nil, err
	}

	// Load all accounts for network
	addresses, err := keyStorage.GetAllAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to load addresses", err)
	}

	log.Printf("construction tester initialized with %d addresses\n", len(addresses))

	// Track balances on all addresses
	for _, address := range addresses {
		balanceStorageHelper.AddInterestingAddress(address)
	}

	// Load prefunded accounts
	var accountBalanceRequests []*utils.AccountBalanceRequest
	for _, prefundedAcc := range config.Construction.PrefundedAccounts {
		address := prefundedAcc.Address
		accountBalance := &utils.AccountBalanceRequest{
			Account: &types.AccountIdentifier{
				Address: address,
			},
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
	)

	coordinatorHandler := processor.NewCoordinatorHandler(
		balanceStorageHelper,
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
		config.SyncConcurrency,
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
		onlineFetcher:    onlineFetcher,
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
		// If a block has yet to be synced, start syncing from tip.
		// TODO: make configurable
		status, fetchErr := t.onlineFetcher.NetworkStatusRetry(ctx, t.network, nil)
		if fetchErr != nil {
			return fmt.Errorf("%w: unable to fetch network status", fetchErr.Err)
		}

		startIndex = status.CurrentBlockIdentifier.Index
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
