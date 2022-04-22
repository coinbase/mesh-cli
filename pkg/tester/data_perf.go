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
	"fmt"
	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"log"
	"time"
)

func Setup_Benchmarking(config *configuration.Configuration) (*fetcher.Fetcher, func() time.Duration, chan time.Duration) {
	// Create a new fetcher
	fetcher := fetcher.New(
		config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	)
	timer := timerFactory()
	elapsed := make(chan time.Duration, 1)
	return fetcher, timer, elapsed
}

// Benchmark the asset issuer's /block endpoint
func Bmark_Block(ctx context.Context, cancel context.CancelFunc, config *configuration.Configuration, fetcher *fetcher.Fetcher, timer func() time.Duration, elapsed chan time.Duration) error {
	go func() {
		for m := config.StartBlock; m < config.EndBlock; m++ {
			for n := 0; n < config.NumTimesToHitEndpoints; n++ {
				partialBlockId := &types.PartialBlockIdentifier{
					Hash:  nil,
					Index: &m,
				}
				_, _ = fetcher.Block(ctx, config.Network, partialBlockId)
			}
		}
		elapsed <- timer()
	}()
	select {
	case <-ctx.Done():
		log.Fatalf("/block endpoint failed check:perf")
	case timeTaken := <-elapsed:
		fmt.Printf("Total Time Taken for /block endpoint for %s times: %s \n", config.NumTimesToHitEndpoints, timeTaken)
		averageTime := timeTaken / time.Duration((int64(config.NumTimesToHitEndpoints) * (config.EndBlock - config.StartBlock)))
		fmt.Printf("Average Time Taken per /block call: %s \n", averageTime)
		return nil
	}
	return nil
}

// Benchmark the asset issuers /account/balance endpoint
func Bmark_AccountBalance(ctx context.Context, cancel context.CancelFunc, config *configuration.Configuration, fetcher *fetcher.Fetcher, timer func() time.Duration, elapsed chan time.Duration) error {
	go func() {
		for m := config.StartBlock; m < config.EndBlock; m++ {
			for n := 0; n < config.NumTimesToHitEndpoints; n++ {
				account := &types.AccountIdentifier{
					Address: "address",
				}
				partialBlockId := &types.PartialBlockIdentifier{
					Hash:  nil,
					Index: &m,
				}
				fetcher.AccountBalance(ctx, config.Network, account, partialBlockId, nil)
			}
		}
		elapsed <- timer()
	}()
	select {
	case <-ctx.Done():
		log.Fatalf("/block endpoint failed check:perf")
	case timeTaken := <-elapsed:
		fmt.Printf("Total Time Taken for /account/balance endpoint for %s times: %s \n", config.NumTimesToHitEndpoints, timeTaken)
		averageTime := timeTaken / time.Duration((int64(config.NumTimesToHitEndpoints) * (config.EndBlock - config.StartBlock)))
		fmt.Printf("Average Time Taken per /block call: %s \n", averageTime)
		return nil
	}
	return nil
}
