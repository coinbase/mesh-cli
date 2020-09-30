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
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

// CheckConstructionResults contains any error that
// occurred on a check:construction run and a collection
// of interesting stats.
type CheckConstructionResults struct {
	Error         string                  `json:"error"`
	EndConditions map[string]int          `json:"end_conditions"`
	Stats         *CheckConstructionStats `json:"stats"`
	// TODO: add test output (like check data)
}

// Print logs CheckConstructionResults to the console.
func (c *CheckConstructionResults) Print() {
	if len(c.Error) > 0 {
		fmt.Printf("\n")
		color.Red("Error: %s", c.Error)
	} else {
		fmt.Printf("\n")
		color.Green("Success: %s", types.PrintStruct(c.EndConditions))
	}

	fmt.Printf("\n")
	c.Stats.Print()
	fmt.Printf("\n")
}

// Output writes CheckConstructionResults to the provided
// path.
func (c *CheckConstructionResults) Output(path string) {
	if len(path) > 0 {
		writeErr := utils.SerializeAndWrite(path, c)
		if writeErr != nil {
			log.Printf("%s: unable to save results\n", writeErr.Error())
		}
	}
}

// ComputeCheckConstructionResults returns a populated
// CheckConstructionResults.
func ComputeCheckConstructionResults(
	cfg *configuration.Configuration,
	err error,
	counterStorage *storage.CounterStorage,
	jobStorage *storage.JobStorage,
) *CheckConstructionResults {
	ctx := context.Background()
	stats := ComputeCheckConstructionStats(ctx, cfg, counterStorage, jobStorage)
	results := &CheckConstructionResults{
		Stats: stats,
	}

	if err != nil {
		results.Error = err.Error()

		// We never want to populate an end condition
		// if there was an error!
		return results
	}

	results.EndConditions = cfg.Construction.EndConditions

	return results
}

// CheckConstructionStats contains interesting stats
// that are tracked while running check:construction.
type CheckConstructionStats struct {
	TransactionsConfirmed int64 `json:"transactions_confirmed"`
	TransactionsCreated   int64 `json:"transactions_created"`
	StaleBroadcasts       int64 `json:"stale_broadcasts"`
	FailedBroadcasts      int64 `json:"failed_broadcasts"`
	AddressesCreated      int64 `json:"addresses_created"`

	WorkflowsCompleted map[string]int64 `json:"workflows_completed"`
}

// PrintCounts logs counter-related stats to the console.
func (c *CheckConstructionStats) PrintCounts() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetRowLine(true)
	table.SetRowSeparator("-")
	table.SetHeader([]string{"check:construction Stats", "Description", "Value"})
	table.Append([]string{
		"Addresses Created",
		"# of addresses created",
		strconv.FormatInt(c.AddressesCreated, 10),
	})
	table.Append([]string{
		"Transactions Created",
		"# of transactions created",
		strconv.FormatInt(c.TransactionsCreated, 10),
	})
	table.Append([]string{
		"Stale Broadcasts",
		"# of broadcasts missing after stale depth",
		strconv.FormatInt(c.StaleBroadcasts, 10),
	})
	table.Append([]string{
		"Transactions Confirmed",
		"# of transactions seen on-chain",
		strconv.FormatInt(c.TransactionsConfirmed, 10),
	})
	table.Append([]string{
		"Failed Broadcasts",
		"# of transactions that exceeded broadcast limit",
		strconv.FormatInt(c.FailedBroadcasts, 10),
	})

	table.Render()
}

// PrintWorkflows logs workflow counts to the console.
func (c *CheckConstructionStats) PrintWorkflows() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetRowLine(true)
	table.SetRowSeparator("-")
	table.SetHeader([]string{"check:construction Workflows", "Count"})
	for workflow, count := range c.WorkflowsCompleted {
		table.Append([]string{
			workflow,
			strconv.FormatInt(count, 10),
		})
	}

	table.Render()
}

// Print calls PrintCounts and PrintWorkflows.
func (c *CheckConstructionStats) Print() {
	c.PrintCounts()
	c.PrintWorkflows()
}

// ComputeCheckConstructionStats returns a populated
// CheckConstructionStats.
func ComputeCheckConstructionStats(
	ctx context.Context,
	config *configuration.Configuration,
	counters *storage.CounterStorage,
	jobs *storage.JobStorage,
) *CheckConstructionStats {
	if counters == nil || jobs == nil {
		return nil
	}

	transactionsCreated, err := counters.Get(ctx, storage.TransactionsCreatedCounter)
	if err != nil {
		log.Printf("%s cannot get transactions created counter\n", err.Error())
		return nil
	}

	transactionsConfirmed, err := counters.Get(ctx, storage.TransactionsConfirmedCounter)
	if err != nil {
		log.Printf("%s cannot get transactions confirmed counter\n", err.Error())
		return nil
	}

	staleBroadcasts, err := counters.Get(ctx, storage.StaleBroadcastsCounter)
	if err != nil {
		log.Printf("%s cannot get stale broadcasts counter\n", err)
		return nil
	}

	failedBroadcasts, err := counters.Get(ctx, storage.FailedBroadcastsCounter)
	if err != nil {
		log.Printf("%s cannot get failed broadcasts counter\n", err.Error())
		return nil
	}

	addressesCreated, err := counters.Get(ctx, storage.AddressesCreatedCounter)
	if err != nil {
		log.Printf("%s cannot get addresses created counter\n", err.Error())
		return nil
	}

	workflowsCompleted := map[string]int64{}
	for _, workflow := range config.Construction.Workflows {
		completed, err := jobs.Completed(ctx, workflow.Name)
		if err != nil {
			log.Printf("%s cannot get completed count for %s\n", err.Error(), workflow.Name)
			return nil
		}

		workflowsCompleted[workflow.Name] = int64(len(completed))
	}

	return &CheckConstructionStats{
		TransactionsCreated:   transactionsCreated.Int64(),
		TransactionsConfirmed: transactionsConfirmed.Int64(),
		StaleBroadcasts:       staleBroadcasts.Int64(),
		FailedBroadcasts:      failedBroadcasts.Int64(),
		AddressesCreated:      addressesCreated.Int64(),
		WorkflowsCompleted:    workflowsCompleted,
	}
}

// CheckConstructionProgress contains the number of
// currently broadcasting transactions and processing
// jobs.
type CheckConstructionProgress struct {
	Broadcasting int `json:"broadcasting"`
	Processing   int `json:"processing"`
}

// ComputeCheckConstructionProgress computes
// *CheckConstructionProgress.
func ComputeCheckConstructionProgress(
	ctx context.Context,
	broadcasts *storage.BroadcastStorage,
	jobs *storage.JobStorage,
) *CheckConstructionProgress {
	inflight, err := broadcasts.GetAllBroadcasts(ctx)
	if err != nil {
		log.Printf("%s cannot get all broadcasts\n", err.Error())
		return nil
	}

	processing, err := jobs.AllProcessing(ctx)
	if err != nil {
		log.Printf("%s cannot get all jobs\n", err.Error())
		return nil
	}

	return &CheckConstructionProgress{
		Broadcasting: len(inflight),
		Processing:   len(processing),
	}
}

// CheckConstructionStatus contains CheckConstructionStats.
type CheckConstructionStatus struct {
	Stats    *CheckConstructionStats    `json:"stats"`
	Progress *CheckConstructionProgress `json:"progress"`
}

// ComputeCheckConstructionStatus returns a populated
// *CheckConstructionStatus.
func ComputeCheckConstructionStatus(
	ctx context.Context,
	config *configuration.Configuration,
	counters *storage.CounterStorage,
	broadcasts *storage.BroadcastStorage,
	jobs *storage.JobStorage,
) *CheckConstructionStatus {
	return &CheckConstructionStatus{
		Stats:    ComputeCheckConstructionStats(ctx, config, counters, jobs),
		Progress: ComputeCheckConstructionProgress(ctx, broadcasts, jobs),
	}
}

// FetchCheckConstructionStatus fetches *CheckConstructionStatus.
func FetchCheckConstructionStatus(URL string) (*CheckConstructionStatus, error) {
	var status CheckConstructionStatus
	if err := JSONFetch(URL, &status); err != nil {
		return nil, fmt.Errorf("%w: unable to fetch construction status", err)
	}

	return &status, nil
}

// ExitConstruction exits check:data, logs the test results to the console,
// and to a provided output path.
func ExitConstruction(
	config *configuration.Configuration,
	counterStorage *storage.CounterStorage,
	jobStorage *storage.JobStorage,
	err error,
	status int,
) {
	results := ComputeCheckConstructionResults(
		config,
		err,
		counterStorage,
		jobStorage,
	)
	if results != nil {
		results.Print()
		results.Output(config.Construction.ResultsOutputFile)
	}

	os.Exit(status)
}
