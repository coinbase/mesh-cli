package cmd

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
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

	network, genesisBlock, operationTypes, operationStatuses, errors, err := newFetcher.Asserter.ClientConfiguration()
	if err != nil {
		log.Fatal(err)
	}

	specFileContents := asserter.FileConfiguration{
		NetworkIdentifier:        network,
		GenesisBlockIdentifier:   genesisBlock,
		AllowedOperationTypes:    operationTypes,
		AllowedOperationStatuses: operationStatuses,
		AllowedErrors:            errors,
	}

	specString := utils.PrettyPrintStruct(specFileContents)
	log.Printf("Spec File: %s\n", specString)

	if err := ioutil.WriteFile(path.Clean(args[0]), []byte(specString), os.FileMode(0600)); err != nil {
		log.Fatal(err)
	}

}
