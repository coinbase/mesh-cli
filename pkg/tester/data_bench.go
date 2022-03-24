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

func Bmark_Sync(ctx context.Context, cancel context.CancelFunc, Config *configuration.Configuration, N int) time.Duration {

	// Create a new fetcher
	fetcher := fetcher.New(
		Config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	)

	dataPath, _ := utils.CreateCommandPath(Config.DataDirectory, dataCmdName, Config.Network)
	logger, err := logger.NewLogger(
		dataPath,
		Config.Data.LogBlocks,
		Config.Data.LogTransactions,
		Config.Data.LogBalanceChanges,
		Config.Data.LogReconciliations,
		logger.Data,
		Config.Network,
	)

	localStore, err := database.NewBadgerDatabase(ctx, dataPath)

	if err != nil {
		logger.Error("%s: cannot initialize database")
	}

	counterStorage := modules.NewCounterStorage(localStore)
	blockStorage := modules.NewBlockStorage(localStore, Config.SerialBlockWorkers)
	balanceStorage := modules.NewBalanceStorage(localStore)

	syncer := statefulsyncer.New(
		ctx,
		Config.Network,
		fetcher,
		blockStorage,
		counterStorage,
		logger,
		cancel,
		[]modules.BlockWorker{balanceStorage},
		statefulsyncer.WithCacheSize(syncer.DefaultCacheSize),
		statefulsyncer.WithMaxConcurrency(Config.MaxSyncConcurrency),
		statefulsyncer.WithPastBlockLimit(Config.MaxReorgDepth),
		statefulsyncer.WithSeenConcurrency(int64(Config.SeenBlockWorkers)),
	)

	startIndex, endIndex := int64(0), int64(10)
	timer := timerFactory()

	for n := 0; n < N; n++ {
		syncer.Sync(ctx, startIndex, endIndex)
	}

	return timer()
}
