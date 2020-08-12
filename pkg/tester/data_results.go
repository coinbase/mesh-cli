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

package tester

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/storage"
	"github.com/coinbase/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

// EndCondition contains the type of
// end condition and any detail associated
// with the stop.
type EndCondition struct {
	Type   configuration.CheckDataEndCondition `json:"type"`
	Detail string                              `json:"detail"`
}

// CheckDataResults contains any error that occurred
// on a check:data run, the outcome of certain tests,
// and a collection of interesting stats.
type CheckDataResults struct {
	Error        string          `json:"error,omitempty"`
	EndCondition *EndCondition   `json:"end_condition"`
	Tests        *CheckDataTests `json:"tests,omitempty"`
	Stats        *CheckDataStats `json:"stats,omitempty"`
}

// Print logs CheckDataResults to the console.
func (c *CheckDataResults) Print() {
	fmt.Printf("\n")
	c.Tests.Print()
	fmt.Printf("\n")
	c.Stats.Print()
	if len(c.Error) > 0 {
		fmt.Printf("\n")
		color.Red("Error: %s", c.Error)
		fmt.Printf("\n")
	}

	if c.EndCondition != nil {
		fmt.Printf("\n")
		color.Green("Success: %s [%s]", c.EndCondition.Type, c.EndCondition.Detail)
		fmt.Printf("\n")
	}
}

// Output writes *CheckDataResults to the provided
// path.
func (c *CheckDataResults) Output(path string) {
	if len(path) > 0 {
		writeErr := utils.SerializeAndWrite(path, c)
		if writeErr != nil {
			log.Printf("%s: unable to save results\n", writeErr.Error())
		}
	}
}

// CheckDataStats contains interesting stats that
// are counted while running the check:data.
type CheckDataStats struct {
	Blocks                  int64   `json:"blocks"`
	Orphans                 int64   `json:"orphans"`
	Transactions            int64   `json:"transactions"`
	Operations              int64   `json:"operations"`
	ActiveReconciliations   int64   `json:"active_reconciliations"`
	InactiveReconciliations int64   `json:"inactive_reconciliations"`
	ReconciliationCoverage  float64 `json:"reconciliation_coverage"`
}

// Print logs CheckDataStats to the console.
func (c *CheckDataStats) Print() {
	if c == nil {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetRowLine(true)
	table.SetRowSeparator("-")
	table.SetHeader([]string{"check:data Stats", "Description", "Value"})
	table.Append([]string{"Blocks", "# of blocks synced", strconv.FormatInt(c.Blocks, 10)})
	table.Append([]string{"Orphans", "# of blocks orphaned", strconv.FormatInt(c.Orphans, 10)})
	table.Append(
		[]string{
			"Transactions",
			"# of transaction processed",
			strconv.FormatInt(c.Transactions, 10),
		},
	)
	table.Append(
		[]string{"Operations", "# of operations processed", strconv.FormatInt(c.Operations, 10)},
	)
	table.Append(
		[]string{
			"Active Reconciliations",
			"# of reconciliations performed after seeing an account in a block",
			strconv.FormatInt(c.ActiveReconciliations, 10),
		},
	)
	table.Append(
		[]string{
			"Inactive Reconciliations",
			"# of reconciliation performed on randomly selected accounts",
			strconv.FormatInt(c.InactiveReconciliations, 10),
		},
	)
	table.Append(
		[]string{
			"Reconciliation Coverage",
			"% of accounts that have been reconciled",
			fmt.Sprintf("%f%%", c.ReconciliationCoverage*utils.OneHundred),
		},
	)

	table.Render()
}

// ComputeCheckDataStats returns a populated CheckDataStats.
func ComputeCheckDataStats(
	ctx context.Context,
	counters *storage.CounterStorage,
	balances *storage.BalanceStorage,
) *CheckDataStats {
	if counters == nil {
		return nil
	}

	blocks, err := counters.Get(ctx, storage.BlockCounter)
	if err != nil {
		log.Printf("%s: cannot get block counter", err.Error())
		return nil
	}

	orphans, err := counters.Get(ctx, storage.OrphanCounter)
	if err != nil {
		log.Printf("%s: cannot get orphan counter", err.Error())
		return nil
	}

	txs, err := counters.Get(ctx, storage.TransactionCounter)
	if err != nil {
		log.Printf("%s: cannot get transaction counter", err.Error())
		return nil
	}

	ops, err := counters.Get(ctx, storage.OperationCounter)
	if err != nil {
		log.Printf("%s: cannot get operations counter", err.Error())
		return nil
	}

	activeReconciliations, err := counters.Get(ctx, storage.ActiveReconciliationCounter)
	if err != nil {
		log.Printf("%s: cannot get active reconciliations counter", err.Error())
		return nil
	}

	inactiveReconciliations, err := counters.Get(ctx, storage.InactiveReconciliationCounter)
	if err != nil {
		log.Printf("%s: cannot get inactive reconciliations counter", err.Error())
		return nil
	}

	stats := &CheckDataStats{
		Blocks:                  blocks.Int64(),
		Orphans:                 orphans.Int64(),
		Transactions:            txs.Int64(),
		Operations:              ops.Int64(),
		ActiveReconciliations:   activeReconciliations.Int64(),
		InactiveReconciliations: inactiveReconciliations.Int64(),
	}

	if balances != nil {
		coverage, err := balances.ReconciliationCoverage(ctx, 0)
		if err != nil {
			log.Printf("%s: cannot get reconcile coverage", err.Error())
			return nil
		}

		stats.ReconciliationCoverage = coverage
	}

	return stats
}

// CheckDataTests indicates which tests passed.
// If a test is nil, it did not apply to the run.
//
// TODO: add CoinTracking
type CheckDataTests struct {
	RequestResponse   bool  `json:"request_response"`
	ResponseAssertion bool  `json:"response_assertion"`
	BlockSyncing      *bool `json:"block_syncing"`
	BalanceTracking   *bool `json:"balance_tracking"`
	Reconciliation    *bool `json:"reconciliation"`
}

// convertBool converts a *bool
// to a test result.
func convertBool(v *bool) string {
	if v == nil {
		return "NOT TESTED"
	}

	if *v {
		return "PASSED"
	}

	return "FAILED"
}

// Print logs CheckDataTests to the console.
func (c *CheckDataTests) Print() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetRowLine(true)
	table.SetRowSeparator("-")
	table.SetHeader([]string{"check:data Tests", "Description", "Status"})
	table.Append(
		[]string{
			"Request/Response",
			"Rosetta implementation serviced all requests",
			convertBool(&c.RequestResponse),
		},
	)
	table.Append(
		[]string{
			"Response Assertion",
			"All responses are correctly formatted",
			convertBool(&c.ResponseAssertion),
		},
	)
	table.Append(
		[]string{
			"Block Syncing",
			"Blocks are connected into a single canonical chain",
			convertBool(c.BlockSyncing),
		},
	)
	table.Append(
		[]string{
			"Balance Tracking",
			"Account balances did not go negative",
			convertBool(c.BalanceTracking),
		},
	)
	table.Append(
		[]string{
			"Reconciliation",
			"No balance discrepencies were found between computed and live balances",
			convertBool(c.Reconciliation),
		},
	)

	table.Render()
}

// RequestResponseTest returns a boolean
// indicating if all endpoints received
// a non-500 response.
func RequestResponseTest(err error) bool {
	if errors.Is(err, fetcher.ErrExhaustedRetries) || errors.Is(err, fetcher.ErrRequestFailed) ||
		errors.Is(err, fetcher.ErrNoNetworks) || errors.Is(err, utils.ErrNetworkNotSupported) {
		return false
	}

	return true
}

// ResponseAssertionTest returns a boolean
// indicating if all responses received from
// the server were correctly formatted.
func ResponseAssertionTest(err error) bool {
	if errors.Is(err, fetcher.ErrAssertionFailed) { // nolint
		return false
	}

	return true
}

// BlockSyncingTest returns a boolean
// indicating if it was possible to sync
// blocks.
func BlockSyncingTest(err error, blocksSynced bool) *bool {
	relatedErrors := []error{
		syncer.ErrCannotRemoveGenesisBlock,
		syncer.ErrOutOfOrder,
		storage.ErrDuplicateKey,
		storage.ErrDuplicateTransactionHash,
	}
	syncPass := true
	for _, relatedError := range relatedErrors {
		if errors.Is(err, relatedError) {
			syncPass = false
			break
		}
	}

	if !blocksSynced && syncPass {
		return nil
	}

	return &syncPass
}

// BalanceTrackingTest returns a boolean
// indicating if any balances went negative
// while syncing.
func BalanceTrackingTest(cfg *configuration.Configuration, err error, operationsSeen bool) *bool {
	relatedErrors := []error{
		storage.ErrNegativeBalance,
	}
	balancePass := true
	for _, relatedError := range relatedErrors {
		if errors.Is(err, relatedError) {
			balancePass = false
			break
		}
	}

	if (cfg.Data.BalanceTrackingDisabled || !operationsSeen) && balancePass {
		return nil
	}

	return &balancePass
}

// ReconciliationTest returns a boolean
// if no reconciliation errors were received.
func ReconciliationTest(
	cfg *configuration.Configuration,
	err error,
	reconciliationsPerformed bool,
) *bool {
	relatedErrors := []error{
		processor.ErrReconciliationFailure,
	}
	reconciliationPass := true
	for _, relatedError := range relatedErrors {
		if errors.Is(err, relatedError) {
			reconciliationPass = false
			break
		}
	}

	if (cfg.Data.BalanceTrackingDisabled || cfg.Data.ReconciliationDisabled || cfg.Data.IgnoreReconciliationError ||
		!reconciliationsPerformed) &&
		reconciliationPass {
		return nil
	}

	return &reconciliationPass
}

// ComputeCheckDataTests returns a populated CheckDataTests.
func ComputeCheckDataTests(
	ctx context.Context,
	cfg *configuration.Configuration,
	err error,
	counterStorage *storage.CounterStorage,
) *CheckDataTests {
	operationsSeen := false
	reconciliationsPerformed := false
	blocksSynced := false
	if counterStorage != nil {
		blocks, err := counterStorage.Get(ctx, storage.BlockCounter)
		if err == nil && blocks.Int64() > 0 {
			blocksSynced = true
		}

		ops, err := counterStorage.Get(ctx, storage.OperationCounter)
		if err == nil && ops.Int64() > 0 {
			operationsSeen = true
		}

		activeReconciliations, err := counterStorage.Get(ctx, storage.ActiveReconciliationCounter)
		if err == nil && activeReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}

		inactiveReconciliations, err := counterStorage.Get(
			ctx,
			storage.InactiveReconciliationCounter,
		)
		if err == nil && inactiveReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}
	}

	return &CheckDataTests{
		RequestResponse:   RequestResponseTest(err),
		ResponseAssertion: ResponseAssertionTest(err),
		BlockSyncing:      BlockSyncingTest(err, blocksSynced),
		BalanceTracking:   BalanceTrackingTest(cfg, err, operationsSeen),
		Reconciliation:    ReconciliationTest(cfg, err, reconciliationsPerformed),
	}
}

// ComputeCheckDataResults returns a populated CheckDataResults.
func ComputeCheckDataResults(
	cfg *configuration.Configuration,
	err error,
	counterStorage *storage.CounterStorage,
	balanceStorage *storage.BalanceStorage,
	endCondition configuration.CheckDataEndCondition,
	endConditionDetail string,
) *CheckDataResults {
	ctx := context.Background()
	tests := ComputeCheckDataTests(ctx, cfg, err, counterStorage)
	stats := ComputeCheckDataStats(ctx, counterStorage, balanceStorage)
	results := &CheckDataResults{
		Tests: tests,
		Stats: stats,
	}

	if err != nil {
		results.Error = err.Error()

		// We never want to populate an end condition
		// if there was an error!
		return results
	}

	if len(endCondition) > 0 {
		results.EndCondition = &EndCondition{
			Type:   endCondition,
			Detail: endConditionDetail,
		}
	}

	return results
}

// Exit exits the program, logs the test results to the console,
// and to a provided output path.
func Exit(
	config *configuration.Configuration,
	counterStorage *storage.CounterStorage,
	balanceStorage *storage.BalanceStorage,
	err error,
	status int,
	endCondition configuration.CheckDataEndCondition,
	endConditionDetail string,
) {
	results := ComputeCheckDataResults(config, err, counterStorage, balanceStorage, endCondition, endConditionDetail)
	results.Print()
	results.Output(config.Data.ResultsOutputFile)

	os.Exit(status)
}
