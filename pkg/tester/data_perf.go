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

package tester

import (
	"context"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"
	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

func SetupBenchmarking(config *configuration.Configuration) (*fetcher.Fetcher, func() time.Duration, chan time.Duration) {
	// Create a new fetcher
	fetcher := fetcher.New(
		config.OnlineURL,
		fetcher.WithMaxRetries(0),
	)
	timer := timerFactory()
	elapsed := make(chan time.Duration, 1)
	return fetcher, timer, elapsed
}

// Benchmark the asset issuer's /block endpoint
func BmarkBlock(ctx context.Context, config *configuration.Configuration, fetcher *fetcher.Fetcher, timer func() time.Duration, elapsed chan time.Duration, rawStats *results.CheckPerfRawStats) error {
	total_errors := 0
	go func() {
		for m := config.Perf.StartBlock; m < config.Perf.EndBlock; m++ {
			for n := 0; n < config.Perf.NumTimesToHitEndpoints; n++ {
				partialBlockId := &types.PartialBlockIdentifier{
					Hash:  nil,
					Index: &m,
				}
				_, err := fetcher.Block(ctx, config.Network, partialBlockId)
				if err != nil {
					total_errors++
				}
			}
		}
		elapsed <- timer()
	}()
	select {
	case <-ctx.Done():
		return cliErrs.ErrBlockBenchmarkTimeout
	case timeTaken := <-elapsed:
		rawStats.BlockEndpointTotalTime = timeTaken
		rawStats.BlockEndpointNumErrors = int64(total_errors)
		return nil
	}
}

// Benchmark the asset issuers /account/balance endpoint
func BmarkAccountBalance(ctx context.Context, config *configuration.Configuration, fetcher *fetcher.Fetcher, timer func() time.Duration, elapsed chan time.Duration, rawStats *results.CheckPerfRawStats) error {
	total_errors := 0
	go func() {
		for m := config.Perf.StartBlock; m < config.Perf.EndBlock; m++ {
			for n := 0; n < config.Perf.NumTimesToHitEndpoints; n++ {
				account := &types.AccountIdentifier{
					Address: "address",
				}
				partialBlockId := &types.PartialBlockIdentifier{
					Hash:  nil,
					Index: &m,
				}
				_, _, _, err := fetcher.AccountBalance(ctx, config.Network, account, partialBlockId, nil)
				if err != nil {
					total_errors++
				}
			}
		}
		elapsed <- timer()
	}()
	select {
	case <-ctx.Done():
		return cliErrs.ErrAccountBalanceBenchmarkTimeout
	case timeTaken := <-elapsed:
		rawStats.AccountBalanceEndpointTotalTime = timeTaken
		rawStats.AccountBalanceNumErrors = int64(total_errors)
		return nil
	}
}
