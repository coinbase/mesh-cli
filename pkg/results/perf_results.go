// Copyright 2022 Coinbase, Inc.
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
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/olekukonko/tablewriter"
)

// Output writes *CheckPerfResults to the provided
// path.
func (c *CheckPerfStats) Output(path string) {
	if len(path) > 0 {
		writeErr := utils.SerializeAndWrite(path, c)
		if writeErr != nil {
			log.Printf("unable to save results: %s\n", writeErr.Error())
		}
	}
}

type CheckPerfRawStats struct {
	BlockEndpointTotalTime          time.Duration
	BlockEndpointNumErrors          int64
	AccountBalanceEndpointTotalTime time.Duration
	AccountBalanceNumErrors         int64
}

// CheckPerfStats contains interesting stats that
// are counted while running the check:perf.
type CheckPerfStats struct {
	StartBlock                          int64 `json:"start_block"`
	EndBlock                            int64 `json:"end_block"`
	NumTimesHitEachEndpoint             int   `json:"num_times_hit_each_endpoint"`
	AccountBalanceEndpointAverageTimeMs int64 `json:"account_balance_endpoint_average_time_ms"`
	AccountBalanceEndpointTotalTimeMs   int64 `json:"account_balance_endpoint_total_time_ms"`
	AccountBalanceEndpointNumErrors     int64 `json:"account_balance_endpoint_num_errors"`
	BlockEndpointAverageTimeMs          int64 `json:"block_endpoint_average_time_ms"`
	BlockEndpointTotalTimeMs            int64 `json:"block_endpoint_total_time_ms"`
	BlockEndpointNumErrors              int64 `json:"block_endpoint_num_errors"`
}

// Print logs CheckPerfStats to the console.
func (c *CheckPerfStats) Print() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetRowLine(true)
	table.SetRowSeparator("-")
	table.SetHeader([]string{"check:perf Stats", "Description", "Value"})
	table.Append([]string{"Start Block", "The Starting Block", strconv.FormatInt(c.StartBlock, 10)})
	table.Append([]string{"End Block", "The Ending Block", strconv.FormatInt(c.EndBlock, 10)})
	table.Append([]string{"Num Times Each Endpoint", "Number of times that each endpoint was hit", strconv.FormatInt(int64(c.NumTimesHitEachEndpoint), 10)})
	table.Append(
		[]string{
			"/Block Endpoint Total Time",
			"Total elapsed time taken to fetch all blocks (ms)",
			strconv.FormatInt(c.BlockEndpointTotalTimeMs, 10),
		},
	)
	table.Append(
		[]string{
			"/Block Endpoint Average Time",
			"Average time taken to fetch each block (ms)",
			strconv.FormatInt(c.BlockEndpointAverageTimeMs, 10),
		},
	)
	table.Append(
		[]string{
			"/Block Endpoint Num Errors",
			"Total num errors occurred while fetching blocks",
			strconv.FormatInt(c.BlockEndpointNumErrors, 10),
		},
	)
	table.Append(
		[]string{
			"/Account/Balance Endpoint Average Time",
			"Average time taken to fetch each account balance (ms)",
			strconv.FormatInt(c.AccountBalanceEndpointAverageTimeMs, 10),
		},
	)
	table.Append(
		[]string{
			"/Account/Balance Endpoint Total Time",
			"Total elapsed time taken to fetch all account balances (ms)",
			strconv.FormatInt(c.AccountBalanceEndpointTotalTimeMs, 10),
		},
	)
	table.Append(
		[]string{
			"/Account/Balance Endpoint Num Errors",
			"Total num errors occurred while fetching account balances",
			strconv.FormatInt(c.AccountBalanceEndpointNumErrors, 10),
		},
	)

	table.Render()
}

// ComputeCheckPerfStats returns a populated CheckPerfStats.
func ComputeCheckPerfStats(
	config *configuration.CheckPerfConfiguration,
	rawStats *CheckPerfRawStats,
) *CheckPerfStats {
	totalNumEndpointsHit := (config.EndBlock - config.StartBlock) * int64(config.NumTimesToHitEndpoints)
	stats := &CheckPerfStats{
		BlockEndpointAverageTimeMs:          rawStats.BlockEndpointTotalTime.Milliseconds() / totalNumEndpointsHit,
		BlockEndpointTotalTimeMs:            rawStats.BlockEndpointTotalTime.Milliseconds(),
		BlockEndpointNumErrors:              rawStats.BlockEndpointNumErrors,
		AccountBalanceEndpointAverageTimeMs: rawStats.AccountBalanceEndpointTotalTime.Milliseconds() / totalNumEndpointsHit,
		AccountBalanceEndpointTotalTimeMs:   rawStats.AccountBalanceEndpointTotalTime.Milliseconds(),
		AccountBalanceEndpointNumErrors:     rawStats.AccountBalanceNumErrors,
		StartBlock:                          config.StartBlock,
		EndBlock:                            config.EndBlock,
		NumTimesHitEachEndpoint:             config.NumTimesToHitEndpoints,
	}

	return stats
}

// ExitPerf exits check:perf, logs the test results to the console,
// and to a provided output path.
func ExitPerf(
	config *configuration.CheckPerfConfiguration,
	err error,
	rawStats *CheckPerfRawStats,
) error {
	if err != nil {
		log.Fatal(fmt.Errorf("Check:Perf Failed!: %w", err))
	}

	stats := ComputeCheckPerfStats(
		config,
		rawStats,
	)

	stats.Print()
	stats.Output(config.StatsOutputFile)

	return err
}
