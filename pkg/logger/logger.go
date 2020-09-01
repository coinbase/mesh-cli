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
	"fmt"
	"log"
	"math/big"
	"os"
	"path"

	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
)

var _ statefulsyncer.Logger = (*Logger)(nil)

const (
	// blockStreamFile contains the stream of processed
	// blocks and whether they were added or removed.
	blockStreamFile = "blocks.txt"

	// transactionStreamFile contains the stream of processed
	// transactions and whether they were added or removed.
	transactionStreamFile = "transactions.txt"

	// balanceStreamFile contains the stream of processed
	// balance changes.
	balanceStreamFile = "balance_changes.txt"

	// reconcileSuccessStreamFile contains the stream of processed
	// reconciliations.
	reconcileSuccessStreamFile = "successful_reconciliations.txt"
	reconcileFailureStreamFile = "failure_reconciliations.txt"

	// addEvent is printed in a stream
	// when an event is added.
	addEvent = "Add"

	// removeEvent is printed in a stream
	// when an event is orphaned.
	removeEvent = "Remove"
)

// Logger contains all logic to record validator output
// and benchmark a Rosetta Server.
type Logger struct {
	logDir            string
	logBlocks         bool
	logTransactions   bool
	logBalanceChanges bool
	logReconciliation bool

	lastStatsMessage string

	CounterStorage *storage.CounterStorage
	BalanceStorage *storage.BalanceStorage
}

// NewLogger constructs a new Logger.
func NewLogger(
	counterStorage *storage.CounterStorage,
	balanceStorage *storage.BalanceStorage,
	logDir string,
	logBlocks bool,
	logTransactions bool,
	logBalanceChanges bool,
	logReconciliation bool,
) *Logger {
	return &Logger{
		CounterStorage:    counterStorage,
		BalanceStorage:    balanceStorage,
		logDir:            logDir,
		logBlocks:         logBlocks,
		logTransactions:   logTransactions,
		logBalanceChanges: logBalanceChanges,
		logReconciliation: logReconciliation,
	}
}

// LogDataStats logs all data values in CounterStorage.
func (l *Logger) LogDataStats(ctx context.Context) error {
	blocks, err := l.CounterStorage.Get(ctx, storage.BlockCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get block counter", err)
	}

	if blocks.Sign() == 0 { // wait for at least 1 block to be processed
		return nil
	}

	orphans, err := l.CounterStorage.Get(ctx, storage.OrphanCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get orphan counter", err)
	}

	txs, err := l.CounterStorage.Get(ctx, storage.TransactionCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get transaction counter", err)
	}

	ops, err := l.CounterStorage.Get(ctx, storage.OperationCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get operations counter", err)
	}

	activeReconciliations, err := l.CounterStorage.Get(ctx, storage.ActiveReconciliationCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get active reconciliations counter", err)
	}

	inactiveReconciliations, err := l.CounterStorage.Get(ctx, storage.InactiveReconciliationCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get inactive reconciliations counter", err)
	}

	statsMessage := fmt.Sprintf(
		"[STATS] Blocks: %s (Orphaned: %s) Transactions: %s Operations: %s",
		blocks.String(),
		orphans.String(),
		txs.String(),
		ops.String(),
	)

	if l.BalanceStorage != nil {
		coverage, err := l.BalanceStorage.ReconciliationCoverage(ctx, 0)
		if err != nil {
			return fmt.Errorf("%w: cannot get reconcile coverage", err)
		}

		statsMessage = fmt.Sprintf(
			"%s Reconciliations: %s (Inactive: %s, Coverage: %f%%)",
			statsMessage,
			new(big.Int).Add(activeReconciliations, inactiveReconciliations).String(),
			inactiveReconciliations.String(),
			coverage*utils.OneHundred,
		)
	}

	// Don't print out the same stats message twice.
	if statsMessage == l.lastStatsMessage {
		return nil
	}

	l.lastStatsMessage = statsMessage
	color.Cyan(statsMessage)

	return nil
}

// LogConstructionStats logs all construction values in CounterStorage.
func (l *Logger) LogConstructionStats(ctx context.Context, inflightTransactions int) error {
	transactionsCreated, err := l.CounterStorage.Get(ctx, storage.TransactionsCreatedCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get transactions created counter", err)
	}

	transactionsConfirmed, err := l.CounterStorage.Get(ctx, storage.TransactionsConfirmedCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get transactions confirmed counter", err)
	}

	staleBroadcasts, err := l.CounterStorage.Get(ctx, storage.StaleBroadcastsCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get stale broadcasts counter", err)
	}

	failedBroadcasts, err := l.CounterStorage.Get(ctx, storage.FailedBroadcastsCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get failed broadcasts counter", err)
	}

	addressesCreated, err := l.CounterStorage.Get(ctx, storage.AddressesCreatedCounter)
	if err != nil {
		return fmt.Errorf("%w cannot get addresses created counter", err)
	}

	statsMessage := fmt.Sprintf(
		"[STATS] Transactions Confirmed: %d (Created: %d, In Progress: %d, Stale: %d, Failed: %d) Addresses Created: %d",
		transactionsConfirmed,
		transactionsCreated,
		inflightTransactions,
		staleBroadcasts,
		failedBroadcasts,
		addressesCreated,
	)
	if statsMessage == l.lastStatsMessage {
		return nil
	}

	l.lastStatsMessage = statsMessage
	color.Cyan(statsMessage)

	return nil
}

// AddBlockStream writes the next processed block to the end of the
// blockStreamFile output file.
func (l *Logger) AddBlockStream(
	ctx context.Context,
	block *types.Block,
) error {
	if !l.logBlocks {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, blockStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	_, err = f.WriteString(fmt.Sprintf(
		"%s Block %d:%s with Parent Block %d:%s\n",
		addEvent,
		block.BlockIdentifier.Index,
		block.BlockIdentifier.Hash,
		block.ParentBlockIdentifier.Index,
		block.ParentBlockIdentifier.Hash,
	))
	if err != nil {
		return err
	}

	return l.TransactionStream(ctx, block)
}

// RemoveBlockStream writes the next processed block to the end of the
// blockStreamFile output file.
func (l *Logger) RemoveBlockStream(
	ctx context.Context,
	block *types.BlockIdentifier,
) error {
	if !l.logBlocks {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, blockStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	_, err = f.WriteString(fmt.Sprintf(
		"%s Block %d:%s\n",
		removeEvent,
		block.Index,
		block.Hash,
	))
	if err != nil {
		return err
	}

	return nil
}

// TransactionStream writes the next processed block's transactions
// to the end of the transactionStreamFile.
func (l *Logger) TransactionStream(
	ctx context.Context,
	block *types.Block,
) error {
	if !l.logTransactions {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, transactionStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	for _, tx := range block.Transactions {
		_, err = f.WriteString(fmt.Sprintf(
			"Transaction %s at Block %d:%s\n",
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
				participant = types.AccountString(op.Account)
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
		}
	}

	return nil
}

// BalanceStream writes a slice of storage.BalanceChanges
// to the balanceStreamFile.
func (l *Logger) BalanceStream(
	ctx context.Context,
	balanceChanges []*parser.BalanceChange,
) error {
	if !l.logBalanceChanges {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, balanceStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	for _, balanceChange := range balanceChanges {
		balanceLog := fmt.Sprintf(
			"Account: %s Change: %s:%s Block: %d:%s",
			balanceChange.Account.Address,
			balanceChange.Difference,
			types.CurrencyString(balanceChange.Currency),
			balanceChange.Block.Index,
			balanceChange.Block.Hash,
		)

		if _, err := f.WriteString(fmt.Sprintf("%s\n", balanceLog)); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileSuccessStream logs all reconciliation checks performed
// during syncing.
func (l *Logger) ReconcileSuccessStream(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	balance string,
	block *types.BlockIdentifier,
) error {
	if !l.logReconciliation {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, reconcileSuccessStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	log.Printf(
		"%s Reconciled %s at %d\n",
		reconciliationType,
		types.AccountString(account),
		block.Index,
	)

	_, err = f.WriteString(fmt.Sprintf(
		"Type:%s Account: %s Currency: %s Balance: %s Block: %d:%s\n",
		reconciliationType,
		types.AccountString(account),
		types.CurrencyString(currency),
		balance,
		block.Index,
		block.Hash,
	))
	if err != nil {
		return err
	}

	return nil
}

// ReconcileFailureStream logs all reconciliation checks performed
// during syncing.
func (l *Logger) ReconcileFailureStream(
	ctx context.Context,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	computedBalance string,
	nodeBalance string,
	block *types.BlockIdentifier,
) error {
	// Always print out reconciliation failures
	if reconciliationType == reconciler.InactiveReconciliation {
		color.Yellow(
			"Missing balance-changing operation detected for %s computed balance: %s%s node balance: %s%s",
			types.AccountString(account),
			computedBalance,
			currency.Symbol,
			nodeBalance,
			currency.Symbol,
		)
	} else {
		color.Yellow(
			"Reconciliation failed for %s at %d computed: %s%s node: %s%s",
			types.AccountString(account),
			block.Index,
			computedBalance,
			currency.Symbol,
			nodeBalance,
			currency.Symbol,
		)
	}

	if !l.logReconciliation {
		return nil
	}

	f, err := os.OpenFile(
		path.Join(l.logDir, reconcileFailureStreamFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return err
	}

	defer closeFile(f)

	_, err = f.WriteString(fmt.Sprintf(
		"Type:%s Account: %s Currency: %s Block: %s:%d computed: %s node: %s\n",
		reconciliationType,
		types.AccountString(account),
		types.CurrencyString(currency),
		block.Hash,
		block.Index,
		computedBalance,
		nodeBalance,
	))
	if err != nil {
		return err
	}

	return nil
}

// Helper function to close log file
func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to close file", err))
	}
}

// LogTransactionCreated logs the hash of created
// transactions.
func LogTransactionCreated(
	transactionIdentifier *types.TransactionIdentifier,
) {
	color.Magenta(
		"Transaction Created: %s\n",
		transactionIdentifier.Hash,
	)
}
