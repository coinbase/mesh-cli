package cmd

import (
	"context"
	"fmt"
	t "github.com/coinbase/rosetta-cli/pkg/tester"
	"github.com/spf13/cobra"
)

var (
	checkBenchmarkCmd = &cobra.Command{
		Use:   "check:benchmark",
		Short: "Benchmark performance of time-critical methods of rosetta-cli and rosetta-sdk",
		Long: `This command can be used to benchmark the performance of time critical methods for rosetta-cli and rosetta-sdk.
This is useful for optimizing the performance of commands like check:data and check:construction, which can take a long time to complete.
The performance improvement can be objectively compared.`,
		RunE: runBenchmarkCmd,
	}
)

func runBenchmarkCmd(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(Context)
	timeTaken := t.Bmark_Sync(ctx, cancel, Config, 30000)
	fmt.Printf("Total Time Taken for Sync Operations: %s", timeTaken)
	return nil
}
