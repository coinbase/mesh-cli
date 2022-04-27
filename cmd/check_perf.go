package cmd

import (
	"context"
	"fmt"
	"github.com/coinbase/rosetta-cli/pkg/results"
	t "github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"time"
)

var (
	checkPerfCmd = &cobra.Command{
		Use:   "check:perf",
		Short: "Benchmark performance of time-critical endpoints of Asset Issuer's Rosetta Implementation",
		Long: `This command can be used to benchmark the performance of time critical methods for a rosetta server.
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

	fetcher, timer, elapsed := t.Setup_Benchmarking(Config)
	blockEndpointTimeConstraint := time.Duration(Config.Perf.BlockEndpointTimeConstraintMs*TotalNumEndpoints) * time.Millisecond
	blockEndpointCtx, blockEndpointCancel := context.WithTimeout(ctx, blockEndpointTimeConstraint)
	g.Go(func() error {
		return t.Bmark_Block(blockEndpointCtx, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer blockEndpointCancel()

	fetcher, timer, elapsed = t.Setup_Benchmarking(Config)
	accountBalanceEndpointTimeConstraint := time.Duration(Config.Perf.AccountBalanceEndpointTimeConstraintMs*TotalNumEndpoints) * time.Millisecond
	accountBalanceEndpointCtx, accountBalanceEndpointCancel := context.WithTimeout(ctx, accountBalanceEndpointTimeConstraint)
	g.Go(func() error {
		return t.Bmark_AccountBalance(accountBalanceEndpointCtx, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer accountBalanceEndpointCancel()

	return results.ExitPerf(Config.Perf, g.Wait(), perfRawStats)
}
