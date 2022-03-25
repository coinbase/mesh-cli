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
		Short: "Generate a zstd dictionary for enhanced compression performance",
		Long: `Zstandard (https://github.com/facebook/zstd) is used by
rosetta-sdk-go/storage to compress data stored to disk. It is possible
to improve compression performance by training a dictionary on a particular
storage namespace. This command runs this training and outputs a dictionary
that can be used with rosetta-sdk-go/storage.

The arguments for this command are:
<namespace> <database path> <dictionary path> <max items> (<existing dictionary path>)

You can learn more about dictionary compression on the Zstandard
website: https://github.com/facebook/zstd#the-case-for-small-data-compression`,
		RunE: runBenchmarkCmd,
	}
)

func runBenchmarkCmd(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(Context)
	timeTaken := t.Bmark_Sync(ctx, cancel, Config, 1000)
	fmt.Printf("Total Time Taken for Sync Operations: %s", timeTaken)
	return nil
}
