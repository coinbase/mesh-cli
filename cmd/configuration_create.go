package cmd

import (
	"log"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/spf13/cobra"
)

var (
	configurationCreateCmd = &cobra.Command{
		Use:   "configuration:create",
		Short: "Create a default configuration file at the provided path",
		Run:   runConfigurationCreateCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runConfigurationCreateCmd(cmd *cobra.Command, args []string) {
	if err := utils.SerializeAndWrite(args[0], configuration.DefaultConfiguration()); err != nil {
		log.Fatalf("%s: unable to save configuration file to %s", err.Error(), args[0])
	}
}
