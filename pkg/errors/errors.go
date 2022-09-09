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
	ErrInitDataTester        = errors.New("unexpected error occurred while trying to initialize data tester")
	ErrReconciliationFailure = errors.New("reconciliation failure")

	// Construction check errors
	ErrConstructionCheckHalt = errors.New("construction check halted")

	// Command errors
	ErrBlockNotFound       = errors.New("block not found")
	ErrNoAvailableNetwork  = errors.New("no networks available")
	ErrAsserterConfigError = errors.New("asserter configuration validation failed")
)
