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

package results

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"

	pkgError "github.com/pkg/errors"

	"github.com/coinbase/rosetta-cli/configuration"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

var (
	f  = false
	tr = true
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
	Error        string          `json:"error"`
	EndCondition *EndCondition   `json:"end_condition"`
	Tests        *CheckDataTests `json:"tests"`
	Stats        *CheckDataStats `json:"stats"`
}

// Print logs CheckDataResults to the console.
func (c *CheckDataResults) Print() {
	if len(c.Error) > 0 {
		fmt.Printf("\n")
		color.Red("Error: %s", c.Error)
	}

	if c.EndCondition != nil {
		fmt.Printf("\n")
		color.Green("Success: %s [%s]", c.EndCondition.Type, c.EndCondition.Detail)
	}

	fmt.Printf("\n")
	if c.Tests != nil {
		c.Tests.Print()
		fmt.Printf("\n")
	}
	if c.Stats != nil {
		c.Stats.Print()
		fmt.Printf("\n")
	}
}

// Output writes *CheckDataResults to the provided
// path.
func (c *CheckDataResults) Output(path string) {
	if len(path) > 0 {
		writeErr := utils.SerializeAndWrite(path, c)
		if writeErr != nil {
			log.Printf("unable to save results: %s\n", writeErr.Error())
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
	Accounts                int64   `json:"accounts"`
	ActiveReconciliations   int64   `json:"active_reconciliations"`
	InactiveReconciliations int64   `json:"inactive_reconciliations"`
	ExemptReconciliations   int64   `json:"exempt_reconciliations"`
	FailedReconciliations   int64   `json:"failed_reconciliations"`
	SkippedReconciliations  int64   `json:"skipped_reconciliations"`
	ReconciliationCoverage  float64 `json:"reconciliation_coverage"`
}

// Print logs CheckDataStats to the console.
func (c *CheckDataStats) Print() {
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
		[]string{"Accounts", "# of accounts seen", strconv.FormatInt(c.Accounts, 10)},
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
			"# of reconciliations performed on randomly selected accounts",
			strconv.FormatInt(c.InactiveReconciliations, 10),
		},
	)
	table.Append(
		[]string{
			"Exempt Reconciliations",
			"# of reconciliation failures considered exempt",
			strconv.FormatInt(c.ExemptReconciliations, 10),
		},
	)
	table.Append(
		[]string{
			"Failed Reconciliations",
			"# of reconciliation failures",
			strconv.FormatInt(c.FailedReconciliations, 10),
		},
	)
	table.Append(
		[]string{
			"Skipped Reconciliations",
			"# of reconciliations skipped",
			strconv.FormatInt(c.SkippedReconciliations, 10),
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
	counters *modules.CounterStorage,
	balances *modules.BalanceStorage,
) *CheckDataStats {
	if counters == nil {
		return nil
	}

	blocks, err := counters.Get(ctx, modules.BlockCounter)
	if err != nil {
		log.Printf("cannot get block counter: %s", err.Error())
		return nil
	}

	orphans, err := counters.Get(ctx, modules.OrphanCounter)
	if err != nil {
		log.Printf("cannot get orphan counter: %s", err.Error())
		return nil
	}

	txs, err := counters.Get(ctx, modules.TransactionCounter)
	if err != nil {
		log.Printf("cannot get transaction counter: %s", err.Error())
		return nil
	}

	ops, err := counters.Get(ctx, modules.OperationCounter)
	if err != nil {
		log.Printf("cannot get operations counter: %s", err.Error())
		return nil
	}

	accounts, err := counters.Get(ctx, modules.SeenAccounts)
	if err != nil {
		log.Printf("cannot get accounts counter: %s", err.Error())
		return nil
	}

	activeReconciliations, err := counters.Get(ctx, modules.ActiveReconciliationCounter)
	if err != nil {
		log.Printf("cannot get active reconciliations counter: %s", err.Error())
		return nil
	}

	inactiveReconciliations, err := counters.Get(ctx, modules.InactiveReconciliationCounter)
	if err != nil {
		log.Printf("cannot get inactive reconciliations counter: %s", err.Error())
		return nil
	}

	exemptReconciliations, err := counters.Get(ctx, modules.ExemptReconciliationCounter)
	if err != nil {
		log.Printf("cannot get exempt reconciliations counter: %s", err.Error())
		return nil
	}

	failedReconciliations, err := counters.Get(ctx, modules.FailedReconciliationCounter)
	if err != nil {
		log.Printf("cannot get failed reconciliations counter: %s", err.Error())
		return nil
	}

	skippedReconciliations, err := counters.Get(ctx, modules.SkippedReconciliationsCounter)
	if err != nil {
		log.Printf("cannot get skipped reconciliations counter: %s", err.Error())
		return nil
	}

	stats := &CheckDataStats{
		Blocks:                  blocks.Int64(),
		Orphans:                 orphans.Int64(),
		Transactions:            txs.Int64(),
		Operations:              ops.Int64(),
		Accounts:                accounts.Int64(),
		ActiveReconciliations:   activeReconciliations.Int64(),
		InactiveReconciliations: inactiveReconciliations.Int64(),
		ExemptReconciliations:   exemptReconciliations.Int64(),
		FailedReconciliations:   failedReconciliations.Int64(),
		SkippedReconciliations:  skippedReconciliations.Int64(),
	}

	if balances != nil {
		coverage, err := balances.EstimatedReconciliationCoverage(ctx)
		switch {
		case err == nil:
			stats.ReconciliationCoverage = coverage
		case errors.Is(err, storageErrs.ErrHelperHandlerMissing):
			// In this case, we use the default 0 value for the reconciliation
			// coverage in stats.
		case err != nil:
			log.Printf("cannot get reconciliation coverage: %s", err.Error())
			return nil
		}
	}

	return stats
}

// CheckDataProgress contains information
// about check:data's syncing progress.
type CheckDataProgress struct {
	Blocks              int64   `json:"blocks"`
	Tip                 int64   `json:"tip"`
	Completed           float64 `json:"completed"`
	Rate                float64 `json:"rate"`
	TimeRemaining       string  `json:"time_remaining"`
	ReconcilerQueueSize int     `json:"reconciler_queue_size"`
	ReconcilerLastIndex int64   `json:"reconciler_last_index"`
}

// ComputeCheckDataProgress returns
// a populated *CheckDataProgress.
func ComputeCheckDataProgress(
	ctx context.Context,
	fetcher *fetcher.Fetcher,
	network *types.NetworkIdentifier,
	counters *modules.CounterStorage,
	blockStorage *modules.BlockStorage,
	reconciler *reconciler.Reconciler,
) *CheckDataProgress {
	networkStatus, fetchErr := fetcher.NetworkStatusRetry(ctx, network, nil)
	if fetchErr != nil {
		fmt.Printf("cannot get network status: %s", fetchErr.Err.Error())
		return nil
	}
	tipIndex := networkStatus.CurrentBlockIdentifier.Index

	genesisBlockIndex := int64(0)
	if networkStatus.GenesisBlockIdentifier != nil {
		genesisBlockIndex = networkStatus.GenesisBlockIdentifier.Index
	}

	// Get current tip in the case that re-orgs occurred
	// or a custom start index was provided.
	headBlock, err := blockStorage.GetHeadBlockIdentifier(ctx)
	if errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
		return nil
	}
	if err != nil {
		fmt.Printf("cannot get head block: %s", err.Error())
		return nil
	}

	blocks, err := counters.Get(ctx, modules.BlockCounter)
	if err != nil {
		fmt.Printf("cannot get block counter: %s", err.Error())
		return nil
	}

	if blocks.Sign() == 0 { // wait for at least 1 block to be processed
		return nil
	}

	orphans, err := counters.Get(ctx, modules.OrphanCounter)
	if err != nil {
		fmt.Printf("cannot get orphan counter: %s", err.Error())
		return nil
	}

	// adjustedBlocks is used to calculate the sync rate (regardless
	// of which block we started syncing at)
	adjustedBlocks := blocks.Int64() - orphans.Int64()
	if tipIndex-adjustedBlocks <= 0 { // return if no blocks to sync
		return nil
	}

	elapsedTime, err := counters.Get(ctx, TimeElapsedCounter)
	if err != nil {
		fmt.Printf("cannot get elapsed time: %s", err.Error())
		return nil
	}

	if elapsedTime.Sign() == 0 { // wait for at least some elapsed time
		return nil
	}

	blocksPerSecond := new(
		big.Float,
	).Quo(
		new(big.Float).SetInt64(adjustedBlocks),
		new(big.Float).SetInt(elapsedTime),
	)
	blocksPerSecondFloat, _ := blocksPerSecond.Float64()

	// some blockchains don't start their genesis block from 0 height
	// So take the height of genesis block and calculate sync percentage based on that
	blocksSynced := new(
		big.Float,
	).Quo(
		new(big.Float).SetInt64(headBlock.Index-genesisBlockIndex),
		new(big.Float).SetInt64(tipIndex-genesisBlockIndex),
	)
	blocksSyncedFloat, _ := blocksSynced.Float64()

	return &CheckDataProgress{
		Blocks:    headBlock.Index,
		Tip:       tipIndex,
		Completed: blocksSyncedFloat * utils.OneHundred,
		Rate:      blocksPerSecondFloat,
		TimeRemaining: utils.TimeToTip(
			blocksPerSecondFloat,
			headBlock.Index,
			tipIndex,
		).String(),
		ReconcilerQueueSize: reconciler.QueueSize(),
		ReconcilerLastIndex: reconciler.LastIndexReconciled(),
	}
}

// CheckDataStatus contains both CheckDataStats
// and CheckDataProgress.
type CheckDataStatus struct {
	Stats    *CheckDataStats    `json:"stats"`
	Progress *CheckDataProgress `json:"progress"`
}

// ComputeCheckDataStatus returns a populated
// *CheckDataStatus.
func ComputeCheckDataStatus(
	ctx context.Context,
	blocks *modules.BlockStorage,
	counters *modules.CounterStorage,
	balances *modules.BalanceStorage,
	fetcher *fetcher.Fetcher,
	network *types.NetworkIdentifier,
	reconciler *reconciler.Reconciler,
) *CheckDataStatus {
	return &CheckDataStatus{
		Stats: ComputeCheckDataStats(
			ctx,
			counters,
			balances,
		),
		Progress: ComputeCheckDataProgress(
			ctx,
			fetcher,
			network,
			counters,
			blocks,
			reconciler,
		),
	}
}

// FetchCheckDataStatus fetches *CheckDataStatus.
func FetchCheckDataStatus(url string) (*CheckDataStatus, error) {
	var status CheckDataStatus
	if err := JSONFetch(url, &status); err != nil {
		return nil, fmt.Errorf("unable to fetch check data status: %w", err)
	}

	return &status, nil
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
			"No balance discrepancies were found between computed and live balances",
			convertBool(c.Reconciliation),
		},
	)

	table.Render()
}

// RequestResponseTest returns a boolean
// indicating if all endpoints received
// a non-500 response.
func RequestResponseTest(err error) bool {
	return !(fetcher.Err(err) ||
		errors.Is(err, utils.ErrNetworkNotSupported))
}

// ResponseAssertionTest returns a boolean
// indicating if all responses received from
// the server were correctly formatted.
func ResponseAssertionTest(err error) bool {
	is, _ := asserter.Err(err)
	return !is
}

// BlockSyncingTest returns a boolean
// indicating if it was possible to sync
// blocks.
func BlockSyncingTest(err error, blocksSynced bool) *bool {
	syncPass := true
	storageFailed, _ := storageErrs.Err(err)
	if syncer.Err(err) ||
		(storageFailed && !errors.Is(err, storageErrs.ErrNegativeBalance)) {
		syncPass = false
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
	balancePass := true
	for _, balanceStorageErr := range storageErrs.BalanceStorageErrs {
		if errors.Is(err, balanceStorageErr) {
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
	reconciliationsFailed bool,
) *bool {
	if errors.Is(err, cliErrs.ErrReconciliationFailure) {
		return &f
	}

	if cfg.Data.BalanceTrackingDisabled ||
		cfg.Data.ReconciliationDisabled ||
		(!reconciliationsPerformed && !reconciliationsFailed) {
		return nil
	}

	if reconciliationsFailed {
		return &f
	}

	return &tr
}

// ComputeCheckDataTests returns a populated CheckDataTests.
func ComputeCheckDataTests( // nolint:gocognit
	ctx context.Context,
	cfg *configuration.Configuration,
	err error,
	counterStorage *modules.CounterStorage,
) *CheckDataTests {
	operationsSeen := false
	reconciliationsPerformed := false
	reconciliationsFailed := false
	blocksSynced := false
	if counterStorage != nil {
		blocks, err := counterStorage.Get(ctx, modules.BlockCounter)
		if err == nil && blocks.Int64() > 0 {
			blocksSynced = true
		}

		ops, err := counterStorage.Get(ctx, modules.OperationCounter)
		if err == nil && ops.Int64() > 0 {
			operationsSeen = true
		}

		activeReconciliations, err := counterStorage.Get(ctx, modules.ActiveReconciliationCounter)
		if err == nil && activeReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}

		inactiveReconciliations, err := counterStorage.Get(
			ctx,
			modules.InactiveReconciliationCounter,
		)
		if err == nil && inactiveReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}

		exemptReconciliations, err := counterStorage.Get(
			ctx,
			modules.ExemptReconciliationCounter,
		)
		if err == nil && exemptReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
		}

		failedReconciliations, err := counterStorage.Get(
			ctx,
			modules.FailedReconciliationCounter,
		)
		if err == nil && failedReconciliations.Int64() > 0 {
			reconciliationsPerformed = true
			reconciliationsFailed = true
		}
	}

	return &CheckDataTests{
		RequestResponse:   RequestResponseTest(err),
		ResponseAssertion: ResponseAssertionTest(err),
		BlockSyncing:      BlockSyncingTest(err, blocksSynced),
		BalanceTracking:   BalanceTrackingTest(cfg, err, operationsSeen),
		Reconciliation: ReconciliationTest(
			cfg,
			err,
			reconciliationsPerformed,
			reconciliationsFailed,
		),
	}
}

// ComputeCheckDataResults returns a populated CheckDataResults.
func ComputeCheckDataResults(
	cfg *configuration.Configuration,
	err error,
	counterStorage *modules.CounterStorage,
	balanceStorage *modules.BalanceStorage,
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
		results.Error = fmt.Sprintf("%+v", err)

		// If all tests pass, but we still encountered an error,
		// then we hard exit without showing check:data results
		// because the error falls beyond our test coverage.
		if tests.RequestResponse &&
			tests.ResponseAssertion &&
			(tests.BlockSyncing == nil || *tests.BlockSyncing) &&
			(tests.BalanceTracking == nil || *tests.BalanceTracking) &&
			(tests.Reconciliation == nil || *tests.Reconciliation) {
			results.Tests = nil
		}

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

// ExitData exits check:data, logs the test results to the console,
// and to a provided output path.
func ExitData(
	config *configuration.Configuration,
	counterStorage *modules.CounterStorage,
	balanceStorage *modules.BalanceStorage,
	err error,
	endCondition configuration.CheckDataEndCondition,
	endConditionDetail string,
) error {
	if !config.ErrorStackTraceDisabled {
		err = pkgError.WithStack(err)
	}

	results := ComputeCheckDataResults(
		config,
		err,
		counterStorage,
		balanceStorage,
		endCondition,
		endConditionDetail,
	)
	if results != nil {
		results.Print()
		results.Output(config.Data.ResultsOutputFile)
	}

	return err
}
