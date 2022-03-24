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
	"github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
	"time"
)

var (
	checkUpdateQueueTime = &cobra.Command{
		Use:   "check:time",
		Short: "Check the correctness of a Rosetta Data API Implementation",
		Long:  `Ch`,
		RunE:  runTime,
	}
)

func elapsed(method string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", method, time.Since(start))
	}
}

func getAsserter() *asserter.Asserter {
	a, _ := asserter.NewClientWithOptions(
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		&types.BlockIdentifier{
			Hash:  "block 0",
			Index: 0,
		},
		[]string{"Transfer"},
		[]*types.OperationStatus{
			{
				Status:     "Success",
				Successful: true,
			},
		},
		[]*types.Error{},
		nil,
		&asserter.Validations{
			Enabled: false,
		},
	)
	return a
}

func runTime(_ *cobra.Command, _ []string) error {
	ensureDataDirectoryExists()
	ctx, cancel := context.WithCancel(Context)

	fetcherOpts := []fetcher.Option{
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime) * time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout) * time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	}

	newFetcher := fetcher.New(
		Config.OnlineURL,
		fetcherOpts...,
	)

	newFetcher.Asserter = getAsserter()

	dataTester := tester.InitializeData(
		ctx,
		Config,
		Config.Network,
		newFetcher,
		cancel,
		nil,
		nil, // only populated when doing recursive search
		&SignalReceived,
	)

	r := dataTester.GetReconciler()
	for i := 1; i < 1000; i++ {
		r.UpdateQueueMap(ctx, nil, 0, true)
	}
	defer elapsed("updateQueueMap")
	return nil
}
