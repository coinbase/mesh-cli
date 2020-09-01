package tester

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

// TODO: add test output (like check data)
type CheckConstructionResults struct {
	Error         string                  `json:"error"`
	EndConditions map[string]int          `json:"end_conditions"`
	Stats         *CheckConstructionStats `json:"stats"`
}

func (c *CheckConstructionResults) Print() {
	if len(c.Error) > 0 {
		fmt.Printf("\n")
		color.Red("Error: %s", c.Error)
	}

	fmt.Printf("\n")
	color.Green("Success: %s", types.PrintStruct(c.EndConditions))

	fmt.Printf("\n")
	c.Stats.Print()
	fmt.Printf("\n")
}

func (c *CheckConstructionResults) Output(path string) {
	if len(path) > 0 {
		writeErr := utils.SerializeAndWrite(path, c)
		if writeErr != nil {
			log.Printf("%s: unable to save results\n", writeErr.Error())
		}
	}
}

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

type CheckConstructionStats struct {
	TransactionsConfirmed int64 `json:"transactions_confirmed"`
	TransactionsCreated   int64 `json:"transactions_created"`
	StaleBroadcasts       int64 `json:"stale_broadcasts"`
	FailedBroadcasts      int64 `json:"failed_broadcasts"`
	AddressesCreated      int64 `json:"addresses_created"`

	WorkflowsCompleted map[string]int64 `json:"workflows_completed"`
}

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

func (c *CheckConstructionStats) Print() {
	c.PrintCounts()
	c.PrintWorkflows()
}

func ComputeCheckConstructionStats(
	ctx context.Context,
	config *configuration.Configuration,
	counters *storage.CounterStorage,
	jobs *storage.JobStorage,
) *CheckConstructionStats {
	if counters == nil {
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
	results.Print()
	results.Output(config.Data.ResultsOutputFile)

	os.Exit(status)
}
