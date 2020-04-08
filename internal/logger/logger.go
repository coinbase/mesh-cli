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

package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/coinbase/rosetta-sdk-go/fetcher"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

const (
	// blockStreamFile contains the stream of processed
	// blocks and whether they were added or removed.
	blockStreamFile = "blocks.txt"

	// transactionStreamFile contains the stream of processed
	// transactions and whether they were added or removed.
	transactionStreamFile = "transactions.txt"

	// accountStreamFile contains the stream of processed
	// balance changes.
	balanceStreamFile = "balances.txt"

	// addEvent is printed in a stream
	// when an event is added.
	addEvent = "Add"

	// removeEvent is printed in a stream
	// when an event is orphaned.
	removeEvent = "Remove"

	// blockLatencyHeader is used as the CSV header
	// to the blockBenchmarkFile.
	blockLatencyHeader = "index,latency,txs,ops\n"

	// accountLatencyHeader is used as the CSV header
	// to the accountBenchmarkFile.
	accountLatencyHeader = "account,latency,balances\n"

	// blockBenchmarkFile contains each block fetch
	// stat in the form of blockLatencyHeader.
	blockBenchmarkFile = "block_benchmarks.csv"

	// accountBenchmarkFile contains each account fetch
	// stat in the form of accountBenchmarkFile.
	accountBenchmarkFile = "account_benchmarks.csv"

	// logFilePermissions specifies that the user can
	// read and write the file.
	logFilePermissions = 0600
)

// Logger contains all logic to record validator ouput
// and benchmark a Rosetta Server.
type Logger struct {
	logDir          string
	logTransactions bool
	logBenchmarks   bool
}

// NewLogger constructs a new Logger.
func NewLogger(logDir string, logTransactions bool, logBenchmarks bool) *Logger {
	return &Logger{
		logDir:          logDir,
		logTransactions: logTransactions,
		logBenchmarks:   logBenchmarks,
	}
}

// BlockStream writes the next processed block to the end of the
// blockStreamFile output file.
func (l *Logger) BlockStream(
	ctx context.Context,
	block *rosetta.Block,
	orphan bool,
) error {
	f, err := os.OpenFile(
		path.Join(l.logDir, blockStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		logFilePermissions,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	verb := addEvent
	if orphan {
		verb = removeEvent
	}

	_, err = f.WriteString(fmt.Sprintf(
		"%s Block %d:%s with Parent Block %d:%s\n",
		verb,
		block.BlockIdentifier.Index,
		block.BlockIdentifier.Hash,
		block.ParentBlockIdentifier.Index,
		block.ParentBlockIdentifier.Hash,
	))
	if err != nil {
		return err
	}

	return l.TransactionStream(ctx, block, verb)
}

// TransactionStream writes the next processed block's transactions
// to the end of the transactionStreamFile.
func (l *Logger) TransactionStream(
	ctx context.Context,
	block *rosetta.Block,
	verb string,
) error {
	if !l.logTransactions {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, transactionStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		logFilePermissions,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, tx := range block.Transactions {
		_, err = f.WriteString(fmt.Sprintf(
			"%s Transaction %s at Block %d:%s\n",
			verb,
			tx.TransactionIdentifier.Hash,
			block.BlockIdentifier.Index,
			block.BlockIdentifier.Hash,
		))
		if err != nil {
			return err
		}

		for _, op := range tx.Operations {
			amount := ""
			symbol := ""
			if op.Amount != nil {
				amount = op.Amount.Value
				symbol = op.Amount.Currency.Symbol
			}
			participant := ""
			if op.Account != nil {
				participant = op.Account.Address
			}

			networkIndex := op.OperationIdentifier.Index
			if op.OperationIdentifier.NetworkIndex != nil {
				networkIndex = *op.OperationIdentifier.NetworkIndex
			}

			_, err = f.WriteString(fmt.Sprintf(
				"TxOp %d(%d) %s %s %s %s %s\n",
				op.OperationIdentifier.Index,
				networkIndex,
				op.Type,
				participant,
				amount,
				symbol,
				op.Status,
			))
			if err != nil {
				return err
			}

			if op.Account != nil && op.Account.Metadata != nil {
				_, err = f.WriteString(fmt.Sprintf("Account Metadata: %+v\n", op.Account.Metadata))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// writeCSVHeader writes a header to a file if it
// doesn't yet exist.
func writeCSVHeader(header string, file string) error {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		f, err := os.OpenFile(
			file,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			logFilePermissions,
		)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = f.WriteString(header)
		return err
	}

	return err
}

// BlockLatency writes the Rosetta Server performance for block fetch
// benchmarks to the block_benchmarks.csv file.
func (l *Logger) BlockLatency(
	ctx context.Context,
	blocks []*fetcher.BlockAndLatency,
) error {
	if !l.logBenchmarks {
		return nil
	}

	file := path.Join(l.logDir, blockBenchmarkFile)
	err := writeCSVHeader(blockLatencyHeader, file)
	if err != nil {
		return err
	}

	// Append to file
	f, err := os.OpenFile(
		file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		logFilePermissions,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, block := range blocks {
		txs := len(block.Block.Transactions)
		ops := 0
		for _, tx := range block.Block.Transactions {
			ops += len(tx.Operations)
		}

		_, err := f.WriteString(fmt.Sprintf(
			"%d,%f,%d,%d\n",
			block.Block.BlockIdentifier.Index,
			block.Latency,
			txs,
			ops,
		))
		if err != nil {
			return err
		}
	}

	return nil
}

// AccountLatency writes the Rosetta Server performance for
// account fetch benchmarks to the account_benchmarks.csv file.
func (l *Logger) AccountLatency(
	ctx context.Context,
	account *rosetta.AccountIdentifier,
	latency float64,
	balances int,
) error {
	if !l.logBenchmarks {
		return nil
	}

	file := path.Join(l.logDir, accountBenchmarkFile)
	err := writeCSVHeader(accountLatencyHeader, file)
	if err != nil {
		return err
	}

	// Append to file
	f, err := os.OpenFile(
		file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		logFilePermissions,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	addressString := account.Address
	if account.SubAccount != nil {
		addressString += account.SubAccount.SubAccount
	}

	_, err = f.WriteString(fmt.Sprintf(
		"%s,%f,%d\n",
		addressString,
		latency,
		balances,
	))

	return err
}

// Network pretty prints the rosetta.NetworkStatusResponse to the console.
func Network(
	ctx context.Context,
	network *rosetta.NetworkStatusResponse,
) error {
	b, err := json.MarshalIndent(network, "", " ")
	if err != nil {
		return err
	}

	fmt.Println("Network Information: " + string(b))

	return nil
}
