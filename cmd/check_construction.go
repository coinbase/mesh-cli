package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var (
	checkConstructionCmd = &cobra.Command{
		Use:   "check:construction",
		Short: "Check the correctness of a Rosetta Construction API Implementation",
		Run:   runCheckConstructionCmd,
	}
)

func runCheckConstructionCmd(cmd *cobra.Command, args []string) {
	log.Fatal("not implemented!")
}
