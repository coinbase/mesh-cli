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
	"encoding/json"
	"io/ioutil"
	"log"
	"path"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/syncer"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	checkAccountCmd = &cobra.Command{
		Use:   "check:account",
		Short: "Debug inactive reconciliation errors for a group of accounts",
		Long: `check:complete identifies accounts with inactive reconciliation
errors (when the balance of an account changes without any operations), however,
it does not identify which block the untracked balance change occurred. This tool
is used for locating exactly which block was missing an operation for a
particular account and currency.

In the future, this tool will be deprecated as check:complete
will automatically identify the block where the missing operation occurred.`,
		Run: runCheckAccountCmd,
	}

	accountFile string
)

func init() {
	checkAccountCmd.Flags().StringVar(
		&accountFile,
		"interesting-accounts",
		"",
		`Absolute path to a file listing all accounts to check on each block. Look
at the examples directory for an example of how to structure this file.`,
	)

	err := checkAccountCmd.MarkFlagRequired("interesting-accounts")
	if err != nil {
		log.Fatal(err)
	}
}

func runCheckAccountCmd(cmd *cobra.Command, args []string) {
	// TODO: unify startup logic with stateless
	ctx, cancel := context.WithCancel(context.Background())

	// Try to load interesting accounts
	interestingAccountsRaw, err := ioutil.ReadFile(path.Clean(accountFile))
	if err != nil {
		log.Fatal(err)
	}

	interestingAccounts := []*reconciler.AccountCurrency{}
	if err := json.Unmarshal(interestingAccountsRaw, &interestingAccounts); err != nil {
		log.Fatal(err)
	}

	accts, err := json.MarshalIndent(interestingAccounts, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Checking: %s\n", string(accts))

	fetcher := fetcher.New(
		ServerURL,
		fetcher.WithBlockConcurrency(BlockConcurrency),
		fetcher.WithTransactionConcurrency(TransactionConcurrency),
	)

	primaryNetwork, _, err := fetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	logger := logger.NewLogger(
		DataDir,
		LogBlocks,
		LogTransactions,
		LogBalanceChanges,
		LogReconciliations,
	)

	g, ctx := errgroup.WithContext(ctx)

	r := reconciler.NewStateless(
		primaryNetwork,
		fetcher,
		logger,
		AccountConcurrency,
		HaltOnReconciliationError,
		interestingAccounts,
	)

	g.Go(func() error {
		return r.Reconcile(ctx)
	})

	syncHandler := syncer.NewBaseHandler(
		logger,
		r,
	)

	statelessSyncer := syncer.NewStateless(
		primaryNetwork,
		fetcher,
		syncHandler,
	)

	g.Go(func() error {
		return syncer.Sync(
			ctx,
			cancel,
			statelessSyncer,
			StartIndex,
			EndIndex,
		)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
