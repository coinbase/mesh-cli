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
	"github.com/slowboat0/rosetta-cli/pkg/constructor"
	"github.com/slowboat0/rosetta-cli/pkg/logger"
	"github.com/slowboat0/rosetta-cli/pkg/processor"
	"github.com/slowboat0/rosetta-cli/pkg/statefulsyncer"
	"github.com/slowboat0/rosetta-cli/pkg/storage"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
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
	constructor      *constructor.Constructor
	broadcastStorage *storage.BroadcastStorage
	blockStorage     *storage.BlockStorage
}

// InitializeConstruction initiates the construction API tester.
func InitializeConstruction(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	onlineFetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
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
		dataPath,
		false,
		false,
		false,
		false,
	)

	blockStorage := storage.NewBlockStorage(localStore)
	keyStorage := storage.NewKeyStorage(localStore)
	coinStorage := storage.NewCoinStorage(localStore, onlineFetcher.Asserter)
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
		config.Construction.ConfirmationDepth,
		config.Construction.StaleDepth,
		config.Construction.BroadcastLimit,
		config.TipDelay,
		config.Construction.BroadcastBehindTip,
		config.Construction.BlockBroadcastLimit,
	)

	broadcastHelper := processor.NewBroadcastStorageHelper(
		network,
		blockStorage,
		onlineFetcher,
	)
	parser := parser.New(onlineFetcher.Asserter, nil)
	broadcastHandler := processor.NewBroadcastStorageHandler(
		config,
		counterStorage,
		parser,
	)

	broadcastStorage.Initialize(broadcastHelper, broadcastHandler)

	offlineFetcher := fetcher.New(
		config.Construction.OfflineURL,
		fetcher.WithAsserter(onlineFetcher.Asserter),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
	)

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

	constructorHelper := processor.NewConstructorHelper(
		offlineFetcher,
		onlineFetcher,
		keyStorage,
		balanceStorage,
		coinStorage,
		broadcastStorage,
	)

	constructorHandler := processor.NewConstructorHandler(
		balanceStorageHelper,
		counterStorage,
	)

	constructor, err := constructor.New(
		config,
		parser,
		constructorHelper,
		constructorHandler,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to create constructor", err)
	}

	syncer := statefulsyncer.New(
		ctx,
		network,
		onlineFetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]storage.BlockWorker{balanceStorage, coinStorage, broadcastStorage},
	)

	return &ConstructionTester{
		network:          network,
		database:         localStore,
		config:           config,
		syncer:           syncer,
		logger:           logger,
		constructor:      constructor,
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
	for ctx.Err() == nil {
		inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
		_ = t.logger.LogConstructionStats(ctx, len(inflight))
		time.Sleep(PeriodicLoggingFrequency)
	}

	// Print stats one last time before exiting
	inflight, _ := t.broadcastStorage.GetAllBroadcasts(ctx)
	_ = t.logger.LogConstructionStats(ctx, len(inflight))

	return ctx.Err()
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
		status, err := t.onlineFetcher.NetworkStatusRetry(ctx, t.network, nil)
		if err != nil {
			return fmt.Errorf("%w: unable to fetch network status", err)
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
	return t.constructor.CreateTransactions(
		ctx,
		t.config.Construction.ClearBroadcasts,
	)
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
