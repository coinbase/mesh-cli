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
	"encoding/json"
	"io/ioutil"
	"log"
	"path"
	"time"

	"github.com/coinbase/rosetta-cli/internal/reconciler"

	"github.com/spf13/cobra"
)

const (
	// ExtendedRetryElapsedTime is used to override the default fetcher
	// retry elapsed time. In practice, extending the retry elapsed time
	// has prevented retry exhaustion errors when many goroutines are
	// used to fetch data from the Rosetta server.
	//
	// TODO: make configurable
	ExtendedRetryElapsedTime = 5 * time.Minute
)

var (
	rootCmd = &cobra.Command{
		Use:   "rosetta-cli",
		Short: "CLI for the Rosetta API",
	}

	// DataDir is a folder used to store logs
	// and any data used to perform validation.
	DataDir string

	// ServerURL is the base URL for a Rosetta
	// server to validate.
	ServerURL string

	// StartIndex is the block index to start syncing.
	StartIndex int64

	// EndIndex is the block index to stop syncing.
	EndIndex int64

	// BlockConcurrency is the concurrency to use
	// while fetching blocks.
	BlockConcurrency uint64

	// TransactionConcurrency is the concurrency to use
	// while fetching transactions (if required).
	TransactionConcurrency uint64

	// AccountConcurrency is the concurrency to use
	// while fetching accounts during reconciliation.
	AccountConcurrency uint64

	// LogBlocks determines if blocks are
	// logged.
	LogBlocks bool

	// LogTransactions determines if transactions are
	// logged.
	LogTransactions bool

	// LogBalanceChanges determines if balance changes are
	// logged.
	LogBalanceChanges bool

	// LogReconciliations determines if reconciliations are
	// logged.
	LogReconciliations bool

	// HaltOnReconciliationError determines if processing
	// should stop when encountering a reconciliation error.
	// It can be beneficial to collect all reconciliation errors
	// during development.
	HaltOnReconciliationError bool

	// ExemptFile is an absolute path to a file listing all accounts
	// to exempt from balance tracking and reconciliation.
	ExemptFile string
)

// Execute handles all invocations of the
// rosetta-cli cmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&DataDir,
		"data-dir",
		"./validator-data",
		"folder used to store logs and any data used to perform validation",
	)
	rootCmd.PersistentFlags().StringVar(
		&ServerURL,
		"server-url",
		"http://localhost:8080",
		"base URL for a Rosetta server to validate",
	)
	rootCmd.PersistentFlags().Int64Var(
		&StartIndex,
		"start",
		-1,
		"block index to start syncing",
	)
	rootCmd.PersistentFlags().Int64Var(
		&EndIndex,
		"end",
		-1,
		"block index to stop syncing",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&BlockConcurrency,
		"block-concurrency",
		8,
		"concurrency to use while fetching blocks",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&TransactionConcurrency,
		"transaction-concurrency",
		16,
		"concurrency to use while fetching transactions (if required)",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&AccountConcurrency,
		"account-concurrency",
		8,
		"concurrency to use while fetching accounts during reconciliation",
	)
	rootCmd.PersistentFlags().BoolVar(
		&LogBlocks,
		"log-blocks",
		false,
		"log processed blocks",
	)
	rootCmd.PersistentFlags().BoolVar(
		&LogTransactions,
		"log-transactions",
		false,
		"log processed transactions",
	)
	rootCmd.PersistentFlags().BoolVar(
		&LogBalanceChanges,
		"log-balance-changes",
		false,
		"log balance changes",
	)
	rootCmd.PersistentFlags().BoolVar(
		&LogReconciliations,
		"log-reconciliations",
		false,
		"log balance reconciliations",
	)
	rootCmd.PersistentFlags().BoolVar(
		&HaltOnReconciliationError,
		"halt-on-reconciliation-error",
		true,
		`Determines if block processing should halt on a reconciliation
error. It can be beneficial to collect all reconciliation errors or silence
reconciliation errors during development.`,
	)
	rootCmd.PersistentFlags().StringVar(
		&ExemptFile,
		"exempt-accounts",
		"",
		`Absolute path to a file listing all accounts to exempt from balance
tracking and reconciliation. Look at the examples directory for an example of
how to structure this file.`,
	)

	rootCmd.AddCommand(checkCompleteCmd)
}

func loadAccounts(filePath string) ([]*reconciler.AccountCurrency, error) {
	if len(filePath) == 0 {
		return []*reconciler.AccountCurrency{}, nil
	}

	accountsRaw, err := ioutil.ReadFile(path.Clean(filePath))
	if err != nil {
		return nil, err
	}

	accounts := []*reconciler.AccountCurrency{}
	if err := json.Unmarshal(accountsRaw, &accounts); err != nil {
		return nil, err
	}

	prettyAccounts, err := json.MarshalIndent(accounts, "", " ")
	if err != nil {
		return nil, err
	}
	log.Printf("Found %d accounts at %s: %s\n", len(accounts), filePath, string(prettyAccounts))

	return accounts, nil
}
