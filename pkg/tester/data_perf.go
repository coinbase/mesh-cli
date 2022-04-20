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
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"time"
)

const (
	startIndex, endIndex int64 = 0, 25
)

// Benchmark the asset issuer's /block endpoint
func Bmark_Block(ctx context.Context, cancel context.CancelFunc, config *configuration.Configuration, numTimesToRun int) time.Duration {
	// Create a new fetcher
	fetcher := fetcher.New(
		config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	)
	timer := timerFactory()

	for m := startIndex; m < endIndex; m++ {
		for n := 0; n < numTimesToRun; n++ {
			partialBlockId := &types.PartialBlockIdentifier{
				Hash:  nil,
				Index: &m,
			}
			_, _ = fetcher.Block(ctx, config.Network, partialBlockId)
		}
	}
	return timer()
}

// Benchmark the asset issuers /account/balance endpoint
func Bmark_AccountBalance(ctx context.Context, cancel context.CancelFunc, config *configuration.Configuration, numTimesToRun int) time.Duration {
	// Create a new fetcher
	fetcher := fetcher.New(
		config.OnlineURL,
		fetcher.WithRetryElapsedTime(time.Duration(config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(config.MaxRetries),
	)
	timer := timerFactory()

	for m := startIndex; m < endIndex; m++ {
		for n := 0; n < numTimesToRun; n++ {
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
	return timer()
}
