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

package cmd

import (
	"context"
	"fmt"
	"time"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"

	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-cli/pkg/tester"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkConstructionCmd = &cobra.Command{
		Use:   "check:construction",
		Short: "Check the correctness of a Rosetta Construction API Implementation",
		Long: `The check:construction command runs an automated test of a
Construction API implementation by creating and broadcasting transactions
on a blockchain. In short, this tool generates new addresses, requests
funds, constructs transactions, signs transactions, broadcasts transactions,
and confirms transactions land on-chain. At each phase, a series of tests
are run to ensure that intermediate representations are correct (for example, does
an unsigned transaction return a superset of operations provided during
construction?).

Check out the https://github.com/coinbase/rosetta-cli/tree/master/examples
directory for examples of how to configure this test for Bitcoin and
Ethereum.

Right now, this tool only supports transfer testing (for both account-based
and UTXO-based blockchains). However, we plan to add support for testing
arbitrary scenarios (for example, staking and governance).`,
		RunE: runCheckConstructionCmd,
	}
)

func runCheckConstructionCmd(_ *cobra.Command, _ []string) error {
	if Config.Construction == nil {
		return results.ExitConstruction(
			Config,
			nil,
			nil,
			cliErrs.ErrConstructionConfigMissing,
		)
	}

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
		return results.ExitConstruction(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to initialize asserter for fetcher: %w", fetchErr.Err),
		)
	}

	_, err := utils.CheckNetworkSupported(ctx, Config.Network, fetcher)
	if err != nil {
		cancel()
		return results.ExitConstruction(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to confirm network %s is supported: %w", types.PrintStruct(Config.Network), err),
		)
	}

	if asserterConfigurationFile != "" {
		if err := validateNetworkOptionsMatchesAsserterConfiguration(
			ctx, fetcher, Config.Network, asserterConfigurationFile,
		); err != nil {
			cancel()
			return results.ExitConstruction(
				Config,
				nil,
				nil,
				fmt.Errorf("network options don't match asserter configuration file %s: %w", asserterConfigurationFile, err),
			)
		}
	}

	constructionTester, err := tester.InitializeConstruction(
		ctx,
		Config,
		Config.Network,
		fetcher,
		cancel,
		&SignalReceived,
	)
	if err != nil {
		return results.ExitConstruction(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to initialize construction tester: %w", err),
		)
	}
	defer constructionTester.CloseDatabase(ctx)

	if err := constructionTester.PerformBroadcasts(ctx); err != nil {
		return results.ExitConstruction(
			Config,
			nil,
			nil,
			fmt.Errorf("unable to perform broadcasts: %w", err),
		)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return constructionTester.StartPeriodicLogger(ctx)
	})

	g.Go(func() error {
		return constructionTester.StartSyncer(ctx, cancel)
	})

	g.Go(func() error {
		return constructionTester.StartConstructor(ctx)
	})

	g.Go(func() error {
		return constructionTester.WatchEndConditions(ctx)
	})

	g.Go(func() error {
		return tester.LogMemoryLoop(ctx)
	})

	g.Go(func() error {
		return tester.StartServer(
			ctx,
			"check:construction status",
			constructionTester,
			Config.Construction.StatusPort,
		)
	})

	sigListeners := []context.CancelFunc{cancel}
	go handleSignals(&sigListeners)

	return constructionTester.HandleErr(g.Wait(), &sigListeners)
}
