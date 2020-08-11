package tester

import (
	"errors"

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

// TestResult indicates which tests passed.
// If a test is nil, it did not apply to the run.
type TestResult struct {
	ResponseCorrectness bool  `json:"response_correctness"`
	BlockSyncing        bool  `json:"block_syncing"`
	BalanceTracking     *bool `json:"balance_tracking,omitempty"`
	CoinTracking        *bool `json:"coin_tracking,omitempty"`
	Reconciliation      *bool `json:"reconciliation,omitempty"`
}

// ResponseCorrectnessPassed returns a boolean
// indicating if all responses received from
// the server were correctly formatted.
func ResponseCorrectnessPassed(err error) bool {
	if errors.Is(err, ErrResponseInvalid) {
		return false
	}

	return true
}

// BlockSyncingPassed returns a boolean
// indicating if it was possible to sync
// blocks.
func BlockSyncingPassed(err error) bool {
	if errors.Is(err, ErrUnableToSync) {
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
func ReconciliationPassed(cfg *configuration.Configuration, err error) *bool {
	if cfg.Data.BalanceTrackingDisabled || cfg.Data.ReconciliationDisabled || cfg.Data.IgnoreReconciliationError {
		return nil
	}

	status := true
	if errors.Is(err, ErrReconciliationFailure) {
		status = false
	}

	return &status
}

// TestStatus returns the status of `check:data`
// based on the error received.
func TestStatus(cfg *configuration.Configuration, err error) *TestResult {
	return &TestResult{
		ResponseCorrectness: ResponseCorrectnessPassed(err),
		BlockSyncing:        BlockSyncingPassed(err),
		BalanceTracking:     BalanceTrackingPassed(cfg, err),
		CoinTracking:        CoinTrackingPassed(cfg, err),
		Reconciliation:      ReconciliationPassed(cfg, err),
	}
}
