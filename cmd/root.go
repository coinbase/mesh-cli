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
	"log"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "rosetta-validator",
		Short: "A simple CLI to interact with a Rosetta server",
	}

	DataDir                string
	ServerURL              string
	BlockConcurrency       uint64
	TransactionConcurrency uint64
	AccountConcurrency     uint64
	LogTransactions        bool
	LogBalances            bool
	LogReconciliation      bool
	BootstrapBalances      string
	StartIndex             int64
	EndIndex               int64
	LookupBalanceByBlock   bool
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&ServerURL,
		"server-url",
		"http://localhost:8080",
		"base url for Rosetta server",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&BlockConcurrency,
		"block-concurrency",
		8,
		"concurrency of block fetches",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&TransactionConcurrency,
		"transaction-concurrency",
		16,
		"concurrency of transaction fetches (if required)",
	)
	rootCmd.PersistentFlags().Uint64Var(
		&AccountConcurrency,
		"account-concurrency",
		8,
		"concurrency of account balance fetches",
	)
	rootCmd.PersistentFlags().BoolVar(
		&LogTransactions,
		"log-transactions",
		true,
		"log processed transactions",
	)
	rootCmd.PersistentFlags().BoolVar(&LogBalances, "log-balances", true, "log balance changes")
	rootCmd.PersistentFlags().BoolVar(
		&LogReconciliation,
		"log-reconciliations",
		true,
		"log reconciliations",
	)
	rootCmd.PersistentFlags().StringVar(
		&DataDir,
		"data-dir",
		"",
		"[required] folder to store all block data and logs",
	)
	if err := rootCmd.MarkPersistentFlagRequired("data-dir"); err != nil {
		log.Fatal(err)
	}

	rootCmd.AddCommand(checkCompleteCmd)
	rootCmd.AddCommand(checkQuickCmd)
}

// TODO
// Commands
// ** keep state and compute
// check:complete (--start) (--finish) //TODO: add support and make sure start error is clear
// ** balance by block only (no duplicate hash checking)
// check:quick (--start) (--finish)
// check:block --hash or --index // TODO: could just use quick with params to do correctly
