// Copyright 2020 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"log"
	"os"

	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/tester"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkConstructionCmd = &cobra.Command{
		Use:   "check:construction",
		Short: "Check the correctness of a Rosetta Construction API Implementation",
		Run:   runCheckConstructionCmd,
	}
)

func runCheckConstructionCmd(cmd *cobra.Command, args []string) {
	ensureDataDirectoryExists()

	// To cancel all execution, need to call multiple cancel functions.
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	localStore, err := storage.NewBadgerStorage(ctx, Config.DataDirectory)
	if err != nil {
		log.Fatalf("%s: unable to initialize database", err.Error())
	}
	defer localStore.Close(ctx)

	keyStorage := storage.NewKeyStorage(localStore)

	t, err := tester.NewConstruction(ctx, Config, keyStorage)
	if err != nil {
		log.Fatalf("%s: unable to initialize construction tester", err.Error())
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return t.StartSyncer(ctx, cancel)
	})

	g.Go(func() error {
		return t.TransferLoop(ctx)
	})

	go handleSignals([]context.CancelFunc{cancel})

	err = g.Wait()
	if SignalReceived {
		color.Red("Check halted")
		os.Exit(1)
		return
	}

	if err != nil {
		color.Red("Check failed: %s", err.Error())
		os.Exit(1)
	}

	// Will only hit this once error conditions are added
	color.Green("Check succeeded")
}
