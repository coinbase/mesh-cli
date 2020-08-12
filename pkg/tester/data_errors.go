package tester

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/storage"

	"github.com/olekukonko/tablewriter"
)

// Root Causes
var (
	ErrResponseInvalid = errors.New("response invalid")
)

// CheckDataResults indicates which tests passed.
// If a test is nil, it did not apply to the run.
type CheckDataResults struct {
	FullError           error
	ResponseCorrectness bool  `json:"response_correctness"`
	BlockSyncing        bool  `json:"block_syncing"`
	BalanceTracking     *bool `json:"balance_tracking,omitempty"`
	Reconciliation      *bool `json:"reconciliation,omitempty"`

	// TODO: add CoinTracking
}

func convertBool(v bool) string {
	if v {
		return "PASSED"
	}

	return "FAILED"
}

// Print writes a table output to the console indicating
// which tests were successful.
func (c *CheckDataResults) Print() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"check:data Tests", "Status"})
	table.Append([]string{"Response Correctness", convertBool(c.ResponseCorrectness)})
	table.Append([]string{"Block Syncing", convertBool(c.BlockSyncing)})

	if c.BalanceTracking != nil {
		table.Append([]string{"Balance Tracking", convertBool(*c.BalanceTracking)})
	}

	if c.Reconciliation != nil {
		table.Append([]string{"Reconciliation", convertBool(*c.Reconciliation)})
	}

	table.Render()

	if c.FullError != nil {
		fmt.Printf("Full Error: %s\n", c.FullError.Error())
	}
}

// ResponseCorrectnessPassed returns a boolean
// indicating if all responses received from
// the server were correctly formatted.
func ResponseCorrectnessPassed(err error) bool {
	if errors.Is(err, ErrResponseInvalid) { // nolint
		return false
	}

	return true
}

// BlockSyncingPassed returns a boolean
// indicating if it was possible to sync
// blocks.
func BlockSyncingPassed(err error) bool {
	if !ResponseCorrectnessPassed(err) {
		return false
	}

	return true
}

// BalanceTrackingPassed returns a boolean
// indicating if any balances went negative
// while syncing.
func BalanceTrackingPassed(cfg *configuration.Configuration, err error, operationsSeen bool) *bool {
	negBalanceErr := errors.Is(err, storage.ErrNegativeBalance)
	if (cfg.Data.BalanceTrackingDisabled || !operationsSeen) && !negBalanceErr {
		return nil
	}

	status := true
	if negBalanceErr {
		status = false
	}

	return &status
}

// ReconciliationPassed returns a boolean
// if no reconciliation errors were received.
func ReconciliationPassed(
	cfg *configuration.Configuration,
	err error,
	reconciliationsPerformed bool,
) *bool {
	recErr := errors.Is(err, processor.ErrReconciliationFailure)
	if (cfg.Data.BalanceTrackingDisabled || cfg.Data.ReconciliationDisabled || cfg.Data.IgnoreReconciliationError ||
		!reconciliationsPerformed) && !recErr {
		return nil
	}

	status := true
	if recErr {
		status = false
	}

	return &status
}

// CheckDataResult returns the status of `check:data`
// based on the error received.
func CheckDataResult(
	cfg *configuration.Configuration,
	err error,
	counterStorage *storage.CounterStorage,
) *CheckDataResults {
	ctx := context.Background()

	operationsSeen := false
	reconciliationsPerformed := false
	if counterStorage != nil {
		ops, err := counterStorage.Get(ctx, storage.OperationCounter)
		if err == nil && ops.Int64() > 0 {
			operationsSeen = true
		}

		activeReconciliations, err := counterStorage.Get(ctx, storage.ActiveReconciliationCounter)
		if err == nil && activeReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}

		inactiveReconciliations, err := counterStorage.Get(ctx, storage.InactiveReconciliationCounter)
		if err == nil && inactiveReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}
	}

	return &CheckDataResults{
		FullError:           err,
		ResponseCorrectness: ResponseCorrectnessPassed(err),
		BlockSyncing:        BlockSyncingPassed(err),
		BalanceTracking:     BalanceTrackingPassed(cfg, err, operationsSeen),
		Reconciliation:      ReconciliationPassed(cfg, err, reconciliationsPerformed),
	}
}
