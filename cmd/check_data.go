// Copyright 2022 Coinbase, Inc.
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

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkDataCmd = &cobra.Command{
		Use:   "check:data",
		Short: "Check the correctness of a Rosetta Data API Implementation",
		Long: `Check all server responses are 
properly constructed, that there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.
		
When re-running this command, it will start where it left off if you specify
some data directory. Otherwise, it will create a new temporary directory and start
again from the genesis block. If you want to discard some number of blocks
populate the --start flag with some block index. Starting from a given index
can be useful to debug a small range of blocks for issues but it is highly
recommended you sync from start to finish to ensure all correctness checks
are performed.
		
By default, account balances are looked up at specific heights (instead of
only at the current block). If your node does not support this functionality
set historical balance disabled to true. This will make reconciliation much
less efficient but it will still work.
		
If check fails due to an INACTIVE reconciliation error (balance changed without
any corresponding operation), the cli will automatically try to find the block
missing an operation. If historical balance disabled is true, this automatic
debugging tool does not work.
		
To debug an INACTIVE account reconciliation error without historical balance lookup,
set the interesting accounts to the path of a JSON file containing
accounts that will be actively checked for balance changes at each block. This
will return an error at the block where a balance change occurred with no
corresponding operations.
		
If your blockchain has a genesis allocation of funds and you set
historical balance disabled to true, you must provide an
absolute path to a JSON file containing initial balances with the
bootstrap balance config. You can look at the examples folder for an example
of what one of these files looks like.`,
		RunE: runCheckDataCmd,
	}
)

func runCheckDataCmd(_ *cobra.Command, _ []string) error {
	ensureDataDirectoryExists()
	ctx, cancel := context.WithCancel(Context)

	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}
	if Config.ForceRetry {
		fetcherOpts = append(fetcherOpts, fetcher.WithForceRetry())
	}

	fetcher := fetcher.New(
		Config.OnlineURL,
		fetcherOpts...,
	)

	_, _, fetchErr := fetcher.InitializeAsserter(ctx, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		cancel()
		return results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to initialize asserter for fetcher: %w", fetchErr.Err),
			"",
			"",
		)
	}

	networkStatus, err := utils.CheckNetworkSupported(ctx, Config.Network, fetcher)
	if err != nil {
		cancel()
		return results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to confirm network %s is supported: %w", types.PrintStruct(Config.Network), err),
			"",
			"",
		)
	}

	if asserterConfigurationFile != "" {
		if err := validateNetworkOptionsMatchesAsserterConfiguration(
			ctx, fetcher, Config.Network, asserterConfigurationFile,
		); err != nil {
			cancel()
			return results.ExitData(
				Config,
				nil,
				nil,
				fmt.Errorf("network options don't match asserter configuration file %s: %w", asserterConfigurationFile, err),
				"",
				"",
			)
		}
	}

	dataTester, err := tester.InitializeData(
		ctx,
		Config,
		Config.Network,
		fetcher,
		cancel,
		networkStatus.GenesisBlockIdentifier,
		nil, // only populated when doing recursive search
		&SignalReceived,
	)
	if err != nil {
		return results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to initialize data tester: %w", err),
			"",
			"",
		)
	}
	defer dataTester.CloseDatabase(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return dataTester.StartPeriodicLogger(ctx)
	})

	g.Go(func() error {
		return dataTester.StartReconciler(ctx)
	})

	g.Go(func() error {
		return dataTester.StartSyncing(ctx)
	})

	g.Go(func() error {
		return dataTester.StartPruning(ctx)
	})

	g.Go(func() error {
		return dataTester.WatchEndConditions(ctx)
	})

	g.Go(func() error {
		return dataTester.StartReconcilerCountUpdater(ctx)
	})

	g.Go(func() error {
		return tester.LogMemoryLoop(ctx)
	})

	g.Go(func() error {
		return tester.StartServer(
			ctx,
			"check:data status",
			dataTester,
			Config.Data.StatusPort,
		)
	})

	sigListeners := []context.CancelFunc{cancel}
	go handleSignals(&sigListeners)

	// HandleErr will exit if we should not attempt
	// to find missing operations.
	return dataTester.HandleErr(g.Wait(), &sigListeners)
}
