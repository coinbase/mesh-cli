package cmd

import (
	"log"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/spf13/cobra"
)

var (
	configurationValidateCmd = &cobra.Command{
		Use:   "configuration:validate",
		Short: "Validate the correctness of a configuration file at the provided path",
		Run:   runConfigurationValidateCmd,
		Args:  cobra.ExactArgs(1),
	}
)

func runConfigurationValidateCmd(cmd *cobra.Command, args []string) {
	_, err := configuration.LoadConfiguration(args[0])
	if err != nil {
		log.Fatalf("%s: unable to save configuration file to %s", err.Error(), args[0])
	}

	log.Println("Configuration file validated!")
}
