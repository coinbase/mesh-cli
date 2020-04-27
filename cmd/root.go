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
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "rosetta-validator",
		Short: "A simple CLI to validate a Rosetta server",
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
)

// Execute handles all invocations of the
// rosetta-validator cmd.
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

	rootCmd.AddCommand(checkCompleteCmd)
	rootCmd.AddCommand(checkQuickCmd)
	rootCmd.AddCommand(checkAccountCmd)
}
