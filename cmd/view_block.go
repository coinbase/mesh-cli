package cmd

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	viewBlockCmd = &cobra.Command{
		Use:   "view:block",
		Short: "",
		Long:  ``,
		Run:   runViewBlockCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runViewBlockCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	index, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to parse index %s", err, args[0]))
	}

	// Create a new fetcher
	newFetcher := fetcher.New(
		ServerURL,
	)

	// Initialize the fetcher's asserter
	//
	// Behind the scenes this makes a call to get the
	// network status and uses the response to inform
	// the asserter what are valid responses.
	primaryNetwork, _, err := newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Print the primary network and network status
	// TODO: support specifying which network to get block from
	log.Printf("Primary Network: %s\n", types.PrettyPrintStruct(primaryNetwork))

	// Fetch the specified block with retries (automatically
	// asserted for correctness)
	//
	// On another note, notice that fetcher.BlockRetry
	// automatically fetches all transactions that are
	// returned in BlockResponse.OtherTransactions. If you use
	// the client directly, you will need to implement a mechanism
	// to fully populate the block by fetching all these
	// transactions.
	block, err := newFetcher.BlockRetry(
		ctx,
		primaryNetwork,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to fetch block", err))
	}

	log.Printf("Current Block: %s\n", types.PrettyPrintStruct(block))
}
