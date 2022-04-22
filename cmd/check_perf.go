package cmd

import (
	"context"
	t "github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/spf13/cobra"
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

	blockEndpointTimeConstraint := time.Duration(Config.BlockEndpointTimeConstraint*Config.NumTimesToHitEndpoints) * time.Millisecond
	blockEndpointCtx, blockEndpointCancel := context.WithTimeout(ctx, blockEndpointTimeConstraint)
	defer blockEndpointCancel()

	accountBalanceEndpointTimeConstraint := time.Duration(Config.AccountBalanceEndpointTimeConstraint*Config.NumTimesToHitEndpoints) * time.Millisecond
	accountBalanceEndpointCtx, accountBalanceEndpointCancel := context.WithTimeout(ctx, accountBalanceEndpointTimeConstraint)
	defer accountBalanceEndpointCancel()

	fetcher, timer, elapsed := t.Setup_Benchmarking(Config)
	go func() {
		t.Bmark_Block(blockEndpointCtx, blockEndpointCancel, Config, fetcher, timer, elapsed)
	}()

	fetcher, timer, elapsed = t.Setup_Benchmarking(Config)
	go func() {
		t.Bmark_AccountBalance(accountBalanceEndpointCtx, accountBalanceEndpointCancel, Config, fetcher, timer, elapsed)
	}()

	return nil
}
