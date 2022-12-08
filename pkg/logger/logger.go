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
	"os"
	"path"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/coinbase/rosetta-cli/pkg/results"

	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
)

var _ statefulsyncer.Logger = (*Logger)(nil)

type CheckType string

type contextKey int

const (
	RequestUUID contextKey = iota

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

	// Construction identifies construction check
	Construction CheckType = "construction"
	// Data identifies data check
	Data CheckType = "data"
)

// Logger contains all logic to record validator output
// and benchmark a Rosetta Server.
type Logger struct {
	logDir            string
	logBlocks         bool
	logTransactions   bool
	logBalanceChanges bool
	logReconciliation bool
	logRequestUUID    string

	lastStatsMessage    string
	lastProgressMessage string

	zapLogger *zap.Logger
}

// NewLogger constructs a new Logger.
func NewLogger(
	logDir string,
	logBlocks bool,
	logTransactions bool,
	logBalanceChanges bool,
	logReconciliation bool,
	checkType CheckType,
	network *types.NetworkIdentifier,
	logRequestUUID string,
	fields ...zap.Field,
) (*Logger, error) {
	zapLogger, err := buildZapLogger(checkType, network, logRequestUUID, fields...)
	if err != nil {
		return nil, fmt.Errorf("failed to build zap logger: %w", err)
	}
	return &Logger{
		logDir:            logDir,
		logBlocks:         logBlocks,
		logTransactions:   logTransactions,
		logBalanceChanges: logBalanceChanges,
		logReconciliation: logReconciliation,
		logRequestUUID:    logRequestUUID,
		zapLogger:         zapLogger,
	}, nil
}

func buildZapLogger(
	checkType CheckType,
	network *types.NetworkIdentifier,
	requestUUID string,
	fields ...zap.Field,
) (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	baseSlice := []zap.Field{
		zap.String("blockchain", network.Blockchain),
		zap.String("network", network.Network),
		zap.String("requestUUID", requestUUID),
		zap.String("check_type", string(checkType)),
	}
	mergedSlice := append(baseSlice, fields...)

	zapLogger, err := config.Build(
		zap.Fields(mergedSlice...),
	)
	return zapLogger, err
}

// LogDataStatus logs results.CheckDataStatus.
func (l *Logger) LogDataStatus(ctx context.Context, status *results.CheckDataStatus) {
	if status.Stats.Blocks == 0 { // wait for at least 1 block to be processed
		return
	}

	statsMessage := fmt.Sprintf(
		"[STATS] Blocks: %d (Orphaned: %d) Transactions: %d Operations: %d Accounts: %d Reconciliations: %d (Inactive: %d, Exempt: %d, Skipped: %d, Coverage: %f%%)", // nolint:lll
		status.Stats.Blocks,
		status.Stats.Orphans,
		status.Stats.Transactions,
		status.Stats.Operations,
		status.Stats.Accounts,
		status.Stats.ActiveReconciliations+status.Stats.InactiveReconciliations,
		status.Stats.InactiveReconciliations,
		status.Stats.ExemptReconciliations,
		status.Stats.SkippedReconciliations,
		status.Stats.ReconciliationCoverage*utils.OneHundred,
	)

	statsMessage = AddRequestUUID(statsMessage, l.logRequestUUID)

	// Don't print out the same stats message twice.
	if statsMessage == l.lastStatsMessage {
		return
	}

	l.lastStatsMessage = statsMessage
	color.Cyan(statsMessage)

	// If Progress is nil, it means we're already done.
	if status.Progress == nil {
		return
	}

	progressMessage := fmt.Sprintf(
		"[PROGRESS] Blocks Synced: %d/%d (Completed: %f%%, Rate: %f/second) Time Remaining: %s Reconciler Queue: %d (Last Index Checked: %d)", // nolint:lll
		status.Progress.Blocks,
		status.Progress.Tip,
		status.Progress.Completed,
		status.Progress.Rate,
		status.Progress.TimeRemaining,
		status.Progress.ReconcilerQueueSize,
		status.Progress.ReconcilerLastIndex,
	)

	progressMessage = AddRequestUUID(progressMessage, l.logRequestUUID)

	// Don't print out the same progress message twice.
	if progressMessage == l.lastProgressMessage {
		return
	}

	l.lastProgressMessage = progressMessage
	color.Cyan(progressMessage)
}

// LogConstructionStatus logs results.CheckConstructionStatus.
func (l *Logger) LogConstructionStatus(
	ctx context.Context,
	status *results.CheckConstructionStatus,
) {
	statsMessage := fmt.Sprintf(
		"[STATS] Transactions Confirmed: %d (Created: %d, In Progress: %d, Stale: %d, Failed: %d) Addresses Created: %d",
		status.Stats.TransactionsConfirmed,
		status.Stats.TransactionsCreated,
		status.Progress.Broadcasting,
		status.Stats.StaleBroadcasts,
		status.Stats.FailedBroadcasts,
		status.Stats.AddressesCreated,
	)
	if statsMessage == l.lastStatsMessage {
		return
	}

	statsMessage = AddRequestUUID(statsMessage, l.logRequestUUID)

	l.lastStatsMessage = statsMessage
	color.Cyan(statsMessage)
}

// LogMemoryStats logs memory usage information.
func LogMemoryStats(ctx context.Context) {
	memUsage := utils.MonitorMemoryUsage(ctx, -1)
	statsMessage := fmt.Sprintf(
		"[MEMORY] Heap: %fMB Stack: %fMB System: %fMB GCs: %d",
		memUsage.Heap,
		memUsage.Stack,
		memUsage.System,
		memUsage.GarbageCollections,
	)
	statsMessage = AddRequestUUIDFromContext(ctx, statsMessage)
	color.Cyan(statsMessage)
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, blockStreamFile), err)
	}

	defer closeFile(f)

	blockString := fmt.Sprintf(
		"%s Block %d:%s with Parent Block %d:%s , RequestUUID: %s\n",
		addEvent,
		block.BlockIdentifier.Index,
		block.BlockIdentifier.Hash,
		block.ParentBlockIdentifier.Index,
		block.ParentBlockIdentifier.Hash,
		l.logRequestUUID,
	)
	fmt.Print(blockString)
	if _, err := f.WriteString(blockString); err != nil {
		return fmt.Errorf("failed to write block string %s: %w", blockString, err)
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, blockStreamFile), err)
	}

	defer closeFile(f)

	blockString := fmt.Sprintf(
		"%s Block %d:%s , RequestUUID: %s\n",
		removeEvent,
		block.Index,
		block.Hash,
		l.logRequestUUID,
	)
	fmt.Print(blockString)
	_, err = f.WriteString(blockString)
	if err != nil {
		return fmt.Errorf("failed to write block string %s: %w", blockString, err)
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, transactionStreamFile), err)
	}

	defer closeFile(f)

	for _, tx := range block.Transactions {
		transactionString := fmt.Sprintf(
			"Transaction %s at Block %d:%s  , RequestUUID: %s\n",
			tx.TransactionIdentifier.Hash,
			block.BlockIdentifier.Index,
			block.BlockIdentifier.Hash,
			l.logRequestUUID,
		)
		fmt.Print(transactionString)
		_, err = f.WriteString(transactionString)
		if err != nil {
			return fmt.Errorf("failed to write transaction string %s: %w", transactionString, err)
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

			transactionOperationString := fmt.Sprintf(
				"TxOp %d(%d) %s %s %s %s %s\n",
				op.OperationIdentifier.Index,
				networkIndex,
				op.Type,
				participant,
				amount,
				symbol,
				*op.Status,
			)
			_, err = f.WriteString(transactionOperationString)
			if err != nil {
				return fmt.Errorf("failed to write transaction operation string %s: %w", transactionOperationString, err)
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, balanceStreamFile), err)
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
		balanceLog = AddRequestUUID(balanceLog, l.logRequestUUID)
		if _, err := f.WriteString(fmt.Sprintf("%s\n", balanceLog)); err != nil {
			return fmt.Errorf("failed to write balance log %s: %w", balanceLog, err)
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, reconcileSuccessStreamFile), err)
	}

	defer closeFile(f)

	log.Printf(
		"%s Reconciled %s at %d\n",
		reconciliationType,
		types.AccountString(account),
		block.Index,
	)

	reconciliationSuccessString := fmt.Sprintf(
		"Type:%s Account: %s Currency: %s Balance: %s Block: %d:%s , RequestUUID: %s\n",
		reconciliationType,
		types.AccountString(account),
		types.CurrencyString(currency),
		balance,
		block.Index,
		block.Hash,
		l.logRequestUUID,
	)
	_, err = f.WriteString(reconciliationSuccessString)
	if err != nil {
		return fmt.Errorf("failed to write reconciliation success string %s: %w", reconciliationSuccessString, err)
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
	liveBalance string,
	block *types.BlockIdentifier,
) error {
	// Always print out reconciliation failures
	if reconciliationType == reconciler.InactiveReconciliation {
		color.Yellow(
			"Missing balance-changing operation detected for %s computed: %s%s live: %s%s",
			types.AccountString(account),
			computedBalance,
			currency.Symbol,
			liveBalance,
			currency.Symbol,
		)
	} else {
		color.Yellow(
			"Reconciliation failed for %s at %d computed: %s%s live: %s%s",
			types.AccountString(account),
			block.Index,
			computedBalance,
			currency.Symbol,
			liveBalance,
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
		return fmt.Errorf("failed to open file %s: %w", path.Join(l.logDir, reconcileFailureStreamFile), err)
	}

	defer closeFile(f)

	reconciliationFailureString := fmt.Sprintf(
		"Type:%s Account: %s Currency: %s Block: %s:%d computed: %s live: %s , RequestUUID: %s\n",
		reconciliationType,
		types.AccountString(account),
		types.CurrencyString(currency),
		block.Hash,
		block.Index,
		computedBalance,
		liveBalance,
		l.logRequestUUID,
	)
	_, err = f.WriteString(reconciliationFailureString)
	if err != nil {
		return fmt.Errorf("failed to write reconciliation failure string %s: %w", reconciliationFailureString, err)
	}

	return nil
}

// Info logs at Info level
func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.zapLogger.Info(msg, fields...)
}

// Debug logs at Debug level
func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.zapLogger.Debug(msg, fields...)
}

// Error logs at Error level
func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.zapLogger.Error(msg, fields...)
}

// Warn logs at Warn level
func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.zapLogger.Warn(msg, fields...)
}

// Panic logs at Panic level
func (l *Logger) Panic(msg string, fields ...zap.Field) {
	l.zapLogger.Panic(msg, fields...)
}

// Fatal logs at Fatal level
func (l *Logger) Fatal(msg string, fields ...zap.Field) {
	l.zapLogger.Fatal(msg, fields...)
}

// Helper function to close log file
func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		log.Fatal(fmt.Errorf("unable to close file: %w", err))
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

// Add requestUUID to the tip
func AddRequestUUIDFromContext(ctx context.Context, msg string) string {
	requestUUID := requestUUIDFromContext(ctx)
	if requestUUID != "" {
		msg = fmt.Sprintf("%s, RequestUUID: %s\n", msg, requestUUID)
	}
	return msg
}

// Add requestUUID to the tip
func AddRequestUUID(msg string, requestUUID string) string {
	if requestUUID != "" {
		msg = fmt.Sprintf("%s, RequestUUID: %s\n", msg, requestUUID)
	}
	return msg
}

// AddRequestUUIDToContext will add a requestUUIDto the context, and return the new context
func AddRequestUUIDToContext(ctx context.Context, uuid string) context.Context {
	return context.WithValue(ctx, RequestUUID, uuid)
}

// requestUUIDFromContext is used to extract a request UUID from a context
func requestUUIDFromContext(ctx context.Context) string {
	switch v := ctx.Value(RequestUUID).(type) {
	case string:
		return v
	default:
		return ""
	}
}
