package logger

import (
	"context"
	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"reflect"
	"testing"
)

func TestLogMemoryStats(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LogMemoryStats(tt.args.ctx)
		})
	}
}

func TestLogTransactionCreated(t *testing.T) {
	type args struct {
		transactionIdentifier *types.TransactionIdentifier
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LogTransactionCreated(tt.args.transactionIdentifier)
		})
	}
}

func TestLogger_AddBlockStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx   context.Context
		block *types.Block
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if err := l.AddBlockStream(tt.args.ctx, tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("AddBlockStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_BalanceStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx            context.Context
		balanceChanges []*parser.BalanceChange
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if err := l.BalanceStream(tt.args.ctx, tt.args.balanceChanges); (err != nil) != tt.wantErr {
				t.Errorf("BalanceStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_Debug(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Debug(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_Error(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Error(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_Fatal(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Fatal(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_Info(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Info(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_LogConstructionStatus(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx    context.Context
		status *results.CheckConstructionStatus
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.LogConstructionStatus(tt.args.ctx, tt.args.status)
		})
	}
}

func TestLogger_LogDataStatus(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx    context.Context
		status *results.CheckDataStatus
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.LogDataStatus(tt.args.ctx, tt.args.status)
		})
	}
}

func TestLogger_Panic(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Panic(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_ReconcileFailureStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx                context.Context
		reconciliationType string
		account            *types.AccountIdentifier
		currency           *types.Currency
		computedBalance    string
		liveBalance        string
		block              *types.BlockIdentifier
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}

			if err := l.ReconcileFailureStream(tt.args.ctx, tt.args.reconciliationType, tt.args.account,
				tt.args.currency, tt.args.computedBalance, tt.args.liveBalance, tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("ReconcileFailureStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_ReconcileSuccessStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx                context.Context
		reconciliationType string
		account            *types.AccountIdentifier
		currency           *types.Currency
		balance            string
		block              *types.BlockIdentifier
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if err := l.ReconcileSuccessStream(tt.args.ctx, tt.args.reconciliationType, tt.args.account,
				tt.args.currency, tt.args.balance, tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("ReconcileSuccessStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_RemoveBlockStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx   context.Context
		block *types.BlockIdentifier
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if err := l.RemoveBlockStream(tt.args.ctx, tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("RemoveBlockStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_TransactionStream(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		ctx   context.Context
		block *types.Block
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if err := l.TransactionStream(tt.args.ctx, tt.args.block); (err != nil) != tt.wantErr {
				t.Errorf("TransactionStream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLogger_Warn(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			l.Warn(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogger_shouldLog(t *testing.T) {
	type fields struct {
		logDir              string
		logLevel            zapcore.Level
		logBlocks           bool
		logTransactions     bool
		logBalanceChanges   bool
		logReconciliation   bool
		lastStatsMessage    string
		lastProgressMessage string
		zapLogger           *zap.Logger
	}
	type args struct {
		logmsg zapcore.Level
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Logger{
				logDir:              tt.fields.logDir,
				logLevel:            tt.fields.logLevel,
				logBlocks:           tt.fields.logBlocks,
				logTransactions:     tt.fields.logTransactions,
				logBalanceChanges:   tt.fields.logBalanceChanges,
				logReconciliation:   tt.fields.logReconciliation,
				lastStatsMessage:    tt.fields.lastStatsMessage,
				lastProgressMessage: tt.fields.lastProgressMessage,
				zapLogger:           tt.fields.zapLogger,
			}
			if got := l.shouldLog(tt.args.logmsg); got != tt.want {
				t.Errorf("shouldLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	type args struct {
		logDir            string
		logBlocks         bool
		logTransactions   bool
		logBalanceChanges bool
		logReconciliation bool
		checkType         CheckType
		network           *types.NetworkIdentifier
		fields            []zap.Field
	}
	tests := []struct {
		name    string
		args    args
		want    *Logger
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLogger(tt.args.logDir, tt.args.logBlocks, tt.args.logTransactions, tt.args.logBalanceChanges,
				tt.args.logReconciliation, zap.DebugLevel, tt.args.checkType, tt.args.network, tt.args.fields...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLogger() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildZapLogger(t *testing.T) {
	type args struct {
		checkType CheckType
		network   *types.NetworkIdentifier
		fields    []zap.Field
	}
	tests := []struct {
		name    string
		args    args
		want    *zap.Logger
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildZapLogger(tt.args.checkType, tt.args.network, tt.args.fields...)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildZapLogger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildZapLogger() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_closeFile(t *testing.T) {
	type args struct {
		f *os.File
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			closeFile(tt.args.f)
		})
	}
}
