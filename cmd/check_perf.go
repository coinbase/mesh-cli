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
	ctx, _ := context.WithCancel(Context)
	g, ctx := errgroup.WithContext(ctx)

	perfRawStats := &results.CheckPerfRawStats{AccountBalanceEndpointTotalTime: -1, BlockEndpointTotalTime: -1}

	fmt.Printf("Running Check:Perf for %s:%s for blocks %d-%d \n", Config.Network.Blockchain, Config.Network.Network, Config.Perf.StartBlock, Config.Perf.EndBlock)

	fetcher, timer, elapsed := t.Setup_Benchmarking(Config)
	blockEndpointTimeConstraint := time.Duration(Config.Perf.BlockEndpointTimeConstraintMs*Config.Perf.NumTimesToHitEndpoints) * time.Millisecond
	_, blockEndpointCancel := context.WithTimeout(ctx, blockEndpointTimeConstraint)
	g.Go(func() error {
		return t.Bmark_Block(ctx, blockEndpointCancel, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer blockEndpointCancel()

	fetcher, timer, elapsed = t.Setup_Benchmarking(Config)
	accountBalanceEndpointTimeConstraint := time.Duration(Config.Perf.AccountBalanceEndpointTimeConstraintMs*Config.Perf.NumTimesToHitEndpoints) * time.Millisecond
	accountBalanceEndpointCtx, accountBalanceEndpointCancel := context.WithTimeout(ctx, accountBalanceEndpointTimeConstraint)
	g.Go(func() error {
		return t.Bmark_AccountBalance(accountBalanceEndpointCtx, accountBalanceEndpointCancel, Config, fetcher, timer, elapsed, perfRawStats)
	})
	defer accountBalanceEndpointCancel()

	results.ExitPerf(Config.Perf, g.Wait(), perfRawStats)

	return nil
}
