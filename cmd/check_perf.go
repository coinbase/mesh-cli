package cmd

import (
	"context"
	"fmt"
	t "github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/spf13/cobra"
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

	timeTaken := t.Bmark_Block(ctx, cancel, Config, 3000)
	fmt.Printf("Total Time Taken for /block endpoint: %s", timeTaken)

	timeTaken = t.Bmark_AccountBalance(ctx, cancel, Config, 3000)
	fmt.Printf("Total Time Taken for /account/balance endpoint: %s", timeTaken)
	return nil
}
