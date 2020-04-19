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

package main

import (
	"context"
	"log"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/storage"
	"github.com/coinbase/rosetta-validator/internal/syncer"

	"github.com/coinbase/rosetta-sdk-go/fetcher"

	"github.com/caarlos0/env"
	"golang.org/x/sync/errgroup"
)

type config struct {
	DataDir                string `env:"DATA_DIR,required"`
	ServerURL              string `env:"SERVER_URL,required"`
	BlockConcurrency       uint64 `env:"BLOCK_CONCURRENCY,required"`
	TransactionConcurrency uint64 `env:"TRANSACTION_CONCURRENCY,required"`
	AccountConcurrency     int    `env:"ACCOUNT_CONCURRENCY,required"`
	LogTransactions        bool   `env:"LOG_TRANSACTIONS,required"`
	LogBalances            bool   `env:"LOG_BALANCES,required"`
	LogReconciliation      bool   `env:"LOG_RECONCILIATION,required"`
	BootstrapBalances      bool   `env:"BOOTSTRAP_BALANCES,required"`
	ReconcileBalances      bool   `env:"RECONCILE_BALANCES,required"`
	NewHeadIndex           int64  `env:"NEW_HEAD_INDEX" envDefault:"-1"`
	LookupBalanceByBlock   bool   `env:"LOOKUP_BALANCE_BY_BLOCK,required"`
}

func main() {
	ctx := context.Background()

	cfg := config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal(err)
	}

	fetcher := fetcher.New(
		ctx,
		cfg.ServerURL,
		fetcher.WithBlockConcurrency(cfg.BlockConcurrency),
		fetcher.WithTransactionConcurrency(cfg.TransactionConcurrency),
	)

	// TODO: sync and reconcile on subnetworks, if they exist.
	primaryNetwork, networkStatus, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	localStore, err := storage.NewBadgerStorage(ctx, cfg.DataDir)
	if err != nil {
		log.Fatal(err)
	}

	blockStorage := storage.NewBlockStorage(ctx, localStore)
	if cfg.BootstrapBalances {
		err = blockStorage.BootstrapBalances(
			ctx,
			cfg.DataDir,
			networkStatus.GenesisBlockIdentifier,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	logger := logger.NewLogger(
		cfg.DataDir,
		cfg.LogTransactions,
		cfg.LogBalances,
		cfg.LogReconciliation,
	)

	g, ctx := errgroup.WithContext(ctx)

	var r *reconciler.Reconciler
	if cfg.ReconcileBalances {
		log.Println("Balance reconciliation enabled")

		r = reconciler.New(
			ctx,
			primaryNetwork,
			blockStorage,
			fetcher,
			logger,
			cfg.AccountConcurrency,
			cfg.LookupBalanceByBlock,
		)

		g.Go(func() error {
			return r.Reconcile(ctx)
		})
	}

	syncHandler := syncer.NewBaseHandler(
		logger,
		r,
	)

	syncer := syncer.New(
		primaryNetwork,
		blockStorage,
		fetcher,
		syncHandler,
	)
	if cfg.NewHeadIndex != -1 {
		err := syncer.NewHeadIndex(ctx, cfg.NewHeadIndex)
		if err != nil {
			log.Fatal(err)
		}
	}

	g.Go(func() error {
		return syncer.Sync(ctx)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
