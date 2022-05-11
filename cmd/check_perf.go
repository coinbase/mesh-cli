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
	t "github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkPerfCmd = &cobra.Command{
		Use:   "check:perf",
		Short: "Benchmark performance of time-critical endpoints of Asset Issuer's Rosetta Implementation",
		Long: `This command can be used to benchmark the performance of time critical methods for a Rosetta server.
This is useful for ensuring that there are no performance degradations in the rosetta-server.`,
		RunE: runCheckPerfCmd,
	}
)

func runCheckPerfCmd(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(Context)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	TotalNumEndpoints := int64(Config.Perf.NumTimesToHitEndpoints) * (Config.Perf.EndBlock - Config.Perf.StartBlock)
	perfRawStats := &results.CheckPerfRawStats{AccountBalanceEndpointTotalTime: -1, BlockEndpointTotalTime: -1}

	fmt.Printf("Running Check:Perf for %s:%s for blocks %d-%d \n", Config.Network.Blockchain, Config.Network.Network, Config.Perf.StartBlock, Config.Perf.EndBlock)

	fetcher, timer, elapsed := t.SetupBenchmarking(Config)
	blockEndpointTimeConstraint := time.Duration(Config.Perf.BlockEndpointTimeConstraintMs*TotalNumEndpoints) * time.Millisecond
	blockEndpointCtx, blockEndpointCancel := context.WithTimeout(ctx, blockEndpointTimeConstraint)
	g.Go(func() error {
		return t.BmarkBlock(blockEndpointCtx, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer blockEndpointCancel()

	fetcher, timer, elapsed = t.SetupBenchmarking(Config)
	accountBalanceEndpointTimeConstraint := time.Duration(Config.Perf.AccountBalanceEndpointTimeConstraintMs*TotalNumEndpoints) * time.Millisecond
	accountBalanceEndpointCtx, accountBalanceEndpointCancel := context.WithTimeout(ctx, accountBalanceEndpointTimeConstraint)
	g.Go(func() error {
		return t.BmarkAccountBalance(accountBalanceEndpointCtx, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer accountBalanceEndpointCancel()

	return results.ExitPerf(Config.Perf, g.Wait(), perfRawStats)
}
