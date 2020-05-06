package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	createSpecCmd = &cobra.Command{
		Use:   "create:spec",
		Short: "",
		Long:  ``,
		Run:   runCreateSpecCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runCreateSpecCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Create a new fetcher
	newFetcher := fetcher.New(
		ServerURL,
	)

	// Initialize the fetcher's asserter
	_, _, err := newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	configuration, err := newFetcher.Asserter.ClientConfiguration()
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to generate spec", err))
	}

	specString := types.PrettyPrintStruct(configuration)
	log.Printf("Spec File: %s\n", specString)

	if err := ioutil.WriteFile(path.Clean(args[0]), []byte(specString), os.FileMode(0600)); err != nil {
		log.Fatal(err)
	}
}
