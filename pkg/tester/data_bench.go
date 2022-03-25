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
	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"time"
)

const (
	startIndex, endIndex int64 = 0, 10
)

func Bmark_Sync(ctx context.Context, cancel context.CancelFunc, config *configuration.Configuration, N int) time.Duration {

	// Create a new fetcher
	fetcher := fetcher.New(
		config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	)

	dataPath, _ := utils.CreateCommandPath(config.DataDirectory, dataCmdName, config.Network)
	logger, _ := logger.NewLogger(
		dataPath,
		config.Data.LogBlocks,
		config.Data.LogTransactions,
		config.Data.LogBalanceChanges,
		config.Data.LogReconciliations,
		logger.Data,
		config.Network,
	)

	localStore, err := database.NewBadgerDatabase(ctx, dataPath)

	if err != nil {
		logger.Error("%s: cannot initialize database")
	}

	counterStorage := modules.NewCounterStorage(localStore)
	blockStorage := modules.NewBlockStorage(localStore, config.SerialBlockWorkers)
	balanceStorage := modules.NewBalanceStorage(localStore)

	syncer := statefulsyncer.New(
		ctx,
		config.Network,
		fetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]modules.BlockWorker{balanceStorage},
		statefulsyncer.WithCacheSize(syncer.DefaultCacheSize),
		statefulsyncer.WithMaxConcurrency(config.MaxSyncConcurrency),
		statefulsyncer.WithPastBlockLimit(config.MaxReorgDepth),
		statefulsyncer.WithSeenConcurrency(int64(config.SeenBlockWorkers)),
	)
	
	timer := timerFactory()

	for n := 0; n < N; n++ {
		_ = syncer.Sync(ctx, startIndex, endIndex)
	}

	return timer()
}
