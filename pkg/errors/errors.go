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

package errors

import (
	"errors"
)

var (
	// Configuration errors
	ErrMultipleDSLFiles                                                  = errors.New("multiple DSL files are found")
	ErrNoDSLFile                                                         = errors.New("no DSL file")
	ErrWrongWorkflowConcurrency                                          = errors.New("reserved workflow concurrency doesn't match")
	ErrNegativeStartIndex                                                = errors.New("start index is negative")
	ErrNegativeEndIndex                                                  = errors.New("end index is negative")
	ErrNegativeReconciliationCoverageIndex                               = errors.New("reconciliation coverage index is negative")
	ErrNegativeReconciliationCoverageAccountCount                        = errors.New("reconciliation coverage account is negative")
	ErrNegativeSeenBlockWorkers                                          = errors.New("the number of seen block workers is negative")
	ErrNegativeSerialBlockWorkers                                        = errors.New("the number of serial block workers is negative")
	ErrReconciliationOutOfRange                                          = errors.New("reconciliation is out of range, it must be in the range [0, 1]")
	ErrTableSizeIsOutOfRange                                             = errors.New("table size is out of range, it must be in the range [2, 100]")
	ErrValueLogFileSizeIsOutOfRange                                      = errors.New("value log file size is out of range, it must be in the range [128, 2048]")
	ErrBalanceTrackingIsDisabledForReconciliation                        = errors.New("balance tracking cannot be disabled for reconciliation")
	ErrBalanceTrackingIsDisabledForReconciliationCoverageEndCondition    = errors.New("balance tracking cannot be disabled for reconciliation coverage end condition")
	ErrReconciliationErrorIsIgnoredForReconciliationCoverageEndCondition = errors.New("reconciliation error cannot be ignored for reconciliation coverage end condition")
	ErrReconciliationIsDisabledForReconciliationCoverageEndCondition     = errors.New("reconciliation cannot be disabled for reconciliation coverage end condition")
	ErrConstructionConfigMissing                                         = errors.New("construction configuration is missing")

	// Data check errors
	ErrDataCheckHalt         = errors.New("data check halted")
	ErrReconciliationFailure = errors.New("reconciliation failure")

	// Spec check errors
	ErrErrorEmptyMessage  = errors.New("error object can't have empty message")
	ErrErrorNegativeCode  = errors.New("error object can't have negative code")
	ErrAccountNullPointer = errors.New("account is nil")
	ErrBlockNotIdempotent = errors.New("multiple calls with the same hash don't return the same block")
	ErrBlockTip           = errors.New("unspecified block_identifier doesn't give the tip block")

	// Construction check errors
	ErrConstructionCheckHalt = errors.New("construction check halted")

	// Command errors
	ErrBlockNotFound                 = errors.New("block not found")
	ErrNoAvailableNetwork            = errors.New("no networks available")
	ErrNetworkOptionsAllowlistIsNil  = errors.New("network options allowlist is nil")
	ErrAsserterConfigurationIsNil    = errors.New("asserter configuration is nil")
	ErrTimestampStartIndexMismatch   = errors.New("timestamp start index mismatch")
	ErrOperationTypeLengthMismatch   = errors.New("operation type length mismatch")
	ErrOperationTypeMismatch         = errors.New("operation type mismatch")
	ErrOperationStatusLengthMismatch = errors.New("operation status length mismatch")
	ErrOperationStatusMismatch       = errors.New("operation status mismatch")
	ErrErrorLengthMismatch           = errors.New("error length mismatch")
	ErrErrorMismatch                 = errors.New("error mismatch")
)
