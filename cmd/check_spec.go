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
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/spf13/cobra"
)

var (
	checkSpecCmd = &cobra.Command{
		Use:   "check:spec",
		Short: "Check a Rosetta implementation satisfies Rosetta spec",
		Long: `Detailed Rosetta spec can be found in https://www.rosetta-api.org/docs/Reference.html.
			Specifically, check:spec will examine the response from all data and construction API endpoints,
			and verifiy they have required fields and the values are properly populated and formatted.`,
		RunE: runCheckSpecCmd,
	}
)

// checkSpec struct should implement following interface
// type checkSpecer interface {
// 	NetworkList() error
// 	NetworkOptions() error
//  NetworkStatus(ctx context.Context) error

// 	AccountBalance() error
// 	AccountCoins() error

// 	Block() error
// 	BlockTransaction() error

// 	ConstructionCombine() error
// 	ConstructionHash() error
// 	ConstructionMetadata() error
// 	ConstructionParse() error
// 	ConstructionPayloads() error
// 	ConstructionPreprocess() error
// 	ConstructionSubmit() error

// 	Error() error
// 	MultipleModes() error
// }
type checkSpec struct {
	onlineFetcher  *fetcher.Fetcher
	offlineFetcher *fetcher.Fetcher
}

func newCheckSpec(ctx context.Context) (*checkSpec, error) {
	onlineFetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}

	offlineFetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.Construction.MaxOfflineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}

	if Config.ForceRetry {
		onlineFetcherOpts = append(onlineFetcherOpts, fetcher.WithForceRetry())
		offlineFetcherOpts = append(offlineFetcherOpts, fetcher.WithForceRetry())
	}

	onlineFetcher := fetcher.New(
		Config.OnlineURL,
		onlineFetcherOpts...,
	)
	offlineFetcher := fetcher.New(
		Config.Construction.OfflineURL,
		offlineFetcherOpts...,
	)

	_, _, fetchErr := onlineFetcher.InitializeAsserter(ctx, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return nil, results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%w: unable to initialize asserter for online node fetcher", fetchErr.Err),
			"",
			"",
		)
	}

	_, _, fetchErr = offlineFetcher.InitializeAsserter(ctx, Config.Network, Config.ValidationFile)
	if fetchErr != nil {
		return nil, results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%w: unable to initialize asserter for offline node fetcher", fetchErr.Err),
			"",
			"",
		)
	}

	return &checkSpec{
		onlineFetcher:  onlineFetcher,
		offlineFetcher: offlineFetcher,
	}, nil
}

func (cs *checkSpec) NetworkOptions(ctx context.Context) error {
	res, err := cs.offlineFetcher.NetworkOptionsRetry(ctx, Config.Network, nil)
	if err != nil {
		return fmt.Errorf("%w: unable to fetch network options", err.Err)
	}

	// version is required
	if res.Version == nil {
		return fmt.Errorf("%w: unable to find version in network/options response", errVersion)
	}

	if err := validateVersion(res.Version.RosettaVersion); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := validateVersion(res.Version.NodeVersion); err != nil {
		return fmt.Errorf("%w", err)
	}

	// allow is required
	if res.Allow == nil {
		return fmt.Errorf("%w: unable to find allow in network/options response", errAllowNullPointer)
	}

	if err := validateOperationStatuses(res.Allow.OperationStatuses); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := validateOperationTypes(res.Allow.OperationTypes); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := validateErrors(res.Allow.Errors); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := validateCallMethods(res.Allow.CallMethods); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := validateBalanceExemptions(res.Allow.BalanceExemptions); err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

func (cs *checkSpec) NetworkStatus(ctx context.Context) error {
	res, err := cs.onlineFetcher.NetworkStatusRetry(ctx, Config.Network, nil)
	if err != nil {
		return fmt.Errorf("%w: unable to fetch network status", err.Err)
	}

	// current_block_identifier is required
	if err := validateBlockIdentifier(res.CurrentBlockIdentifier); err != nil {
		return fmt.Errorf("%w", err)
	}

	// current_block_timestamp is required
	if err := validateTimestamp(res.CurrentBlockTimestamp); err != nil {
		return fmt.Errorf("%w", err)
	}

	// genesis_block_identifier is required
	if err := validateBlockIdentifier(res.GenesisBlockIdentifier); err != nil {
		return fmt.Errorf("%w", err)
	}

	// peers is required
	if err := validatePeers(res.Peers); err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

func (cs *checkSpec) NetworkList(ctx context.Context, fetcher *fetcher.Fetcher) error {
	networks, err := fetcher.NetworkList(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: unable to fetch network list", err.Err)
	}
	if len(networks.NetworkIdentifiers) == 0 {
		return fmt.Errorf("network_identifiers are required")
	}
	for _, network := range networks.NetworkIdentifiers {
		if network.Network == Config.Network.Network &&
			network.Blockchain == Config.Network.Blockchain {
			return nil
		}
	}
	return fmt.Errorf("network identifier in configuration file is not returned by /network/list")
}

func runCheckSpecCmd(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	cs, err := newCheckSpec(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to create checkSpec object with online URL", err)
	}

	if err = cs.NetworkStatus(ctx); err != nil {
		return fmt.Errorf("%w: network status verification failed", err)
	}

	if err = cs.NetworkList(ctx, cs.onlineFetcher); err != nil {
		return fmt.Errorf("%w: online network list verification failed", err)
	}

	if err = cs.NetworkList(ctx, cs.offlineFetcher); err != nil {
		return fmt.Errorf("%w: offline network list verification failed", err)
	}

	// TODO: more checks
	if err != nil {
		return fmt.Errorf("%w: unable to create checkSpec object with offline URL", err)
	}

	if err = cs.NetworkOptions(ctx); err != nil {
		return fmt.Errorf("%w: network options verification failed", err)
	}

	fmt.Println("Successfully validated check:spec")
	return nil
}
