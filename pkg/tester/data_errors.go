package tester

import (
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-cli/configuration"
)

// Root Causes
var (
	ErrResponseInvalid       = errors.New("response invalid")
	ErrUnableToSync          = errors.New("unable to sync")
	ErrBalanceInvalid        = errors.New("balance invalid")
	ErrCoinInvalid           = errors.New("coin invalid")
	ErrReconciliationFailure = errors.New("reconciliation failure")
)

// CheckDataResults indicates which tests passed.
// If a test is nil, it did not apply to the run.
type CheckDataResults struct {
	FullError           error
	ResponseCorrectness bool  `json:"response_correctness"`
	BlockSyncing        bool  `json:"block_syncing"`
	BalanceTracking     *bool `json:"balance_tracking,omitempty"`
	CoinTracking        *bool `json:"coin_tracking,omitempty"`
	Reconciliation      *bool `json:"reconciliation,omitempty"`
}

func (c *CheckDataResults) String() string {
	outputString := fmt.Sprintf("Full Error: %s\n", c.FullError.Error())
	outputString = fmt.Sprintf("%s**** Test Results ****\n", outputString)
	outputString = fmt.Sprintf("%sResponse Correctness: %t\n", outputString, c.ResponseCorrectness)
	outputString = fmt.Sprintf("%sBlock Syncing: %t\n", outputString, c.BlockSyncing)

	if c.BalanceTracking != nil {
		outputString = fmt.Sprintf("%sBalance Tracking: %t\n", outputString, *c.BalanceTracking)
	}

	if c.CoinTracking != nil {
		outputString = fmt.Sprintf("%sCoin Tracking: %t\n", outputString, *c.CoinTracking)
	}

	if c.Reconciliation != nil {
		outputString = fmt.Sprintf("%sReconciliation: %t\n", outputString, *c.Reconciliation)
	}

	return outputString
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
	if errors.Is(err, ErrUnableToSync) || !ResponseCorrectnessPassed(err) {
		return false
	}

	return true
}

// BalanceTrackingPassed returns a boolean
// indicating if any balances went negative
// while syncing.
func BalanceTrackingPassed(cfg *configuration.Configuration, err error) *bool {
	if cfg.Data.BalanceTrackingDisabled {
		return nil
	}

	status := true
	if errors.Is(err, ErrBalanceInvalid) {
		status = false
	}

	return &status
}

// CoinTrackingPassed returns a boolean
// indicating if coins could not be tracked.
// Note: this will always return true for
// account-based blockchains.
func CoinTrackingPassed(cfg *configuration.Configuration, err error) *bool {
	if cfg.Data.CoinTrackingDisabled {
		return nil
	}

	status := true
	if errors.Is(err, ErrCoinInvalid) {
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
	if cfg.Data.BalanceTrackingDisabled || cfg.Data.ReconciliationDisabled || cfg.Data.IgnoreReconciliationError ||
		!reconciliationsPerformed {
		return nil
	}

	status := true
	if errors.Is(err, ErrReconciliationFailure) {
		status = false
	}

	return &status
}

// CheckDataResult returns the status of `check:data`
// based on the error received.
func CheckDataResult(
	cfg *configuration.Configuration,
	err error,
	reconciliationsPerformed bool,
) *CheckDataResults {
	return &CheckDataResults{
		FullError:           err,
		ResponseCorrectness: ResponseCorrectnessPassed(err),
		BlockSyncing:        BlockSyncingPassed(err),
		BalanceTracking:     BalanceTrackingPassed(cfg, err),
		CoinTracking:        CoinTrackingPassed(cfg, err),
		Reconciliation:      ReconciliationPassed(cfg, err, reconciliationsPerformed),
	}
}
