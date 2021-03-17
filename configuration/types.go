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

package configuration

import (
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// CheckDataEndCondition is a type of "successful" end
// for the "check:data" method.
type CheckDataEndCondition string

const (
	// IndexEndCondition is used to indicate that the index end condition
	// has been met.
	IndexEndCondition CheckDataEndCondition = "Index End Condition"

	// DurationEndCondition is used to indicate that the duration
	// end condition has been met.
	DurationEndCondition CheckDataEndCondition = "Duration End Condition"

	// TipEndCondition is used to indicate that the tip end condition
	// has been met.
	TipEndCondition CheckDataEndCondition = "Tip End Condition"

	// ReconciliationCoverageEndCondition is used to indicate that the reconciliation
	// coverage end condition has been met.
	ReconciliationCoverageEndCondition CheckDataEndCondition = "Reconciliation Coverage End Condition"
)

// Default Configuration Values
const (
	DefaultURL                               = "http://localhost:8080"
	DefaultTimeout                           = 10
	DefaultMaxRetries                        = 5
	DefaultMaxOnlineConnections              = 120 // most OS have a default limit of 128
	DefaultMaxOfflineConnections             = 4   // we shouldn't need many connections for construction
	DefaultMaxSyncConcurrency                = 64
	DefaultActiveReconciliationConcurrency   = 16
	DefaultInactiveReconciliationConcurrency = 4
	DefaultInactiveReconciliationFrequency   = 250
	DefaultConfirmationDepth                 = 10
	DefaultStaleDepth                        = 30
	DefaultBroadcastLimit                    = 3
	DefaultTipDelay                          = 300
	DefaultBlockBroadcastLimit               = 5
	DefaultStatusPort                        = 9090
	DefaultMaxReorgDepth                     = 100

	// ETH Defaults
	EthereumIDBlockchain = "Ethereum"
	EthereumIDNetwork    = "Ropsten"
)

// Default Configuration Values
var (
	EthereumNetwork = &types.NetworkIdentifier{
		Blockchain: EthereumIDBlockchain,
		Network:    EthereumIDNetwork,
	}
)

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	// OfflineURL is the URL of a Rosetta API implementation in "offline mode".
	OfflineURL string `json:"offline_url"`

	// MaxOffineConnections is the maximum number of open connections that the offline
	// fetcher will open.
	MaxOfflineConnections int `json:"max_offline_connections"`

	// ForceRetry overrides the default retry handling to retry
	// on all non-200 responses.
	ForceRetry bool `json:"force_retry,omitempty"`

	// StaleDepth is the number of blocks to wait before attempting
	// to rebroadcast after not finding a transaction on-chain.
	StaleDepth int64 `json:"stale_depth"`

	// BroadcastLimit is the number of times to attempt re-broadcast
	// before giving up on a transaction broadcast.
	BroadcastLimit int `json:"broadcast_limit"`

	// IgnoreBroadcastFailures determines if we should exit when there
	// are broadcast failures (that surpass the BroadcastLimit).
	IgnoreBroadcastFailures bool `json:"ignore_broadcast_failures"`

	// ClearBroadcasts indicates if all pending broadcasts should
	// be removed from BroadcastStorage on restart.
	ClearBroadcasts bool `json:"clear_broadcasts"`

	// BroadcastBehindTip indicates if we should broadcast transactions
	// when we are behind tip (as defined by TipDelay).
	BroadcastBehindTip bool `json:"broadcast_behind_tip"`

	// BlockBroadcastLimit is the number of transactions to attempt
	// broadcast in a single block. When there are many pending
	// broadcasts, it may make sense to limit the number of broadcasts.
	BlockBroadcastLimit int `json:"block_broadcast_limit"`

	// RebroadcastAll indicates if all pending broadcasts should be
	// rebroadcast from BroadcastStorage on restart.
	RebroadcastAll bool `json:"rebroadcast_all"`

	// PrefundedAccounts is an array of prefunded accounts
	// to use while testing.
	PrefundedAccounts []*modules.PrefundedAccount `json:"prefunded_accounts,omitempty"`

	// Workflows are executed by the rosetta-cli to test
	// certain construction flows.
	Workflows []*job.Workflow `json:"workflows"`

	// ConstructorDSLFile is the path relative to the configuration file
	// of a Rosetta Constructor DSL file (*.ros)
	// that describes which Workflows to test.
	//
	// DSL Spec: https://github.com/coinbase/rosetta-sdk-go/tree/master/constructor/dsl
	ConstructorDSLFile string `json:"constructor_dsl_file"`

	// EndConditions is a map of workflow:count that
	// indicates how many of each workflow should be performed
	// before check:construction should stop. For example,
	// {"create_account": 5} indicates that 5 "create_account"
	// workflows should be performed before stopping.
	EndConditions map[string]int `json:"end_conditions,omitempty"`

	// StatusPort allows the caller to query a running check:construction
	// test to get stats about progress. This can be used instead
	// of parsing logs to populate some sort of status dashboard.
	StatusPort uint `json:"status_port,omitempty"`

	// ResultsOutputFile is the absolute filepath of where to save
	// the results of a check:construction run.
	ResultsOutputFile string `json:"results_output_file,omitempty"`

	// Quiet is a boolean indicating if all request and response
	// logging should be silenced.
	Quiet bool `json:"quiet,omitempty"`

	// InitialBalanceFetchDisabled configures rosetta-cli
	// not to lookup the balance of newly seen accounts at the
	// parent block before applying operations. Disabling this
	// is only a good idea if you create multiple new accounts each block
	// and don't fund any accounts before starting check:construction.
	//
	// This is a separate config from the data config because it
	// is usually false whereas the data config by the same name is usually true.
	InitialBalanceFetchDisabled bool `json:"initial_balance_fetch_disabled"`
}

// ReconciliationCoverage is used to add conditions
// to reconciliation coverage for exiting `check:data`.
// All provided conditions must be satisfied before
// the end condition is considered satisfied.
//
// If FromTip, Tip, Height, and AccountCount are not provided,
// `check:data` will halt as soon as coverage surpasses
// Coverage.
type ReconciliationCoverage struct {
	// Coverage is some value [0.0, 1.0] that represents
	// the % of accounts reconciled.
	Coverage float64 `json:"coverage"`

	// FromTip is a boolean indicating if reconciliation coverage
	// should only be measured from tip (i.e. reconciliations
	// performed at or after tip was reached).
	FromTip bool `json:"from_tip,omitempty"`

	// Tip is a boolean indicating that tip must be reached
	// before reconciliation coverage is considered valid.
	Tip bool `json:"tip,omitempty"`

	// Index is an int64 indicating the height that must be
	// reached before reconciliation coverage is considered valid.
	Index *int64 `json:"index,omitempty"`

	// AccountCount is an int64 indicating the number of accounts
	// that must be observed before reconciliation coverage is considered
	// valid.
	AccountCount *int64 `json:"account_count,omitempty"`
}

// DataEndConditions contains all the conditions for the syncer to stop
// when running check:data. If any one of these conditions is considered
// true, `check:data` will stop with success.
type DataEndConditions struct {
	// Index configures the syncer to stop once reaching a particular block height.
	Index *int64 `json:"index,omitempty"`

	// Tip configures the syncer to stop once it reached the tip.
	// Make sure to configure `tip_delay` if you use this end
	// condition.
	Tip *bool `json:"tip,omitempty"`

	// Duration configures the syncer to stop after running
	// for Duration seconds.
	Duration *uint64 `json:"duration,omitempty"`

	// ReconciliationCoverage configures the syncer to stop once it reaches
	// some level of reconciliation coverage.
	ReconciliationCoverage *ReconciliationCoverage `json:"reconciliation_coverage,omitempty"`
}

// DataConfiguration contains all configurations to run check:data.
type DataConfiguration struct {
	// ActiveReconciliationConcurrency is the concurrency to use while fetching accounts
	// during active reconciliation.
	ActiveReconciliationConcurrency uint64 `json:"active_reconciliation_concurrency"`

	// InactiveReconciliationConcurrency is the concurrency to use while fetching accounts
	// during inactive reconciliation.
	InactiveReconciliationConcurrency uint64 `json:"inactive_reconciliation_concurrency"`

	// InactiveReconciliationFrequency is the number of blocks to wait between
	// inactive reconiliations on each account.
	InactiveReconciliationFrequency uint64 `json:"inactive_reconciliation_frequency"`

	// LogBlocks is a boolean indicating whether to log processed blocks.
	LogBlocks bool `json:"log_blocks"`

	// LogTransactions is a boolean indicating whether to log processed transactions.
	LogTransactions bool `json:"log_transactions"`

	// LogBalanceChanges is a boolean indicating whether to log all balance changes.
	LogBalanceChanges bool `json:"log_balance_changes"`

	// LogReconciliations is a boolean indicating whether to log all reconciliations.
	LogReconciliations bool `json:"log_reconciliations"`

	// IgnoreReconciliationError determines if block processing should halt on a reconciliation
	// error. It can be beneficial to collect all reconciliation errors or silence
	// reconciliation errors during development.
	IgnoreReconciliationError bool `json:"ignore_reconciliation_error"`

	// ExemptAccounts is a path relative to the configuration file
	// to a file listing all accounts to exempt from balance
	// tracking and reconciliation. Look at the examples directory for an example of
	// how to structure this file.
	ExemptAccounts string `json:"exempt_accounts"`

	// BootstrapBalances is a path relative to the configuration file to a file used
	// to bootstrap balances before starting syncing. If this value is populated after
	// beginning syncing, it will be ignored.
	BootstrapBalances string `json:"bootstrap_balances"`

	// HistoricalBalanceEnabled is a boolean that dictates how balance lookup is performed.
	// When set to true, balances are looked up at the block where a balance
	// change occurred instead of at the current block. Blockchains that do not support
	// historical balance lookup should set this to false.
	HistoricalBalanceEnabled *bool `json:"historical_balance_enabled,omitempty"`

	// InterestingAccounts is a path to a file listing all accounts to check on each block. Look
	// at the examples directory for an example of how to structure this file.
	InterestingAccounts string `json:"interesting_accounts"`

	// ReconciliationDisabled is a boolean that indicates reconciliation should not
	// be attempted. When first testing an implementation, it can be useful to disable
	// some of the more advanced checks to confirm syncing is working as expected.
	ReconciliationDisabled bool `json:"reconciliation_disabled"`

	// ReconciliationDrainDisabled is a boolean that configures the rosetta-cli
	// to exit check:data before the entire active reconciliation queue has
	// been drained (if reconciliation is enabled).
	ReconciliationDrainDisabled bool `json:"reconciliation_drain_disabled"`

	// InactiveDiscrepencySearchDisabled is a boolean indicating if a search
	// should be performed to find any inactive reconciliation discrepencies.
	// Note, a search will never be performed if historical balance lookup
	// is disabled.
	InactiveDiscrepencySearchDisabled bool `json:"inactive_discrepency_search_disabled"`

	// BalanceTrackingDisabled is a boolean that indicates balances calculation
	// should not be attempted. When first testing an implemenation, it can be
	// useful to just try to fetch all blocks before checking for balance
	// consistency.
	BalanceTrackingDisabled bool `json:"balance_tracking_disabled"`

	// CoinTrackingDisabled is a boolean that indicates coin (or UTXO) tracking
	// should not be attempted. When first testing an implemenation, it can be
	// useful to just try to fetch all blocks before checking for coin
	// consistency.
	CoinTrackingDisabled bool `json:"coin_tracking_disabled"`

	// StartIndex is the block height to start syncing from. If no StartIndex
	// is provided, syncing will start from the last saved block.
	// If no blocks have ever been synced, syncing will start from genesis.
	StartIndex *int64 `json:"start_index,omitempty"`

	// EndCondition contains the conditions for the syncer to stop
	EndConditions *DataEndConditions `json:"end_conditions,omitempty"`

	// StatusPort allows the caller to query a running check:data
	// test to get stats about progress. This can be used instead
	// of parsing logs to populate some sort of status dashboard.
	StatusPort uint `json:"status_port,omitempty"`

	// ResultsOutputFile is the absolute filepath of where to save
	// the results of a check:data run.
	ResultsOutputFile string `json:"results_output_file"`

	// PruningDisabled is a bolean that indicates storage pruning should
	// not be attempted. This should really only ever be set to true if you
	// wish to use `start_index` at a later point to restart from some
	// previously synced block.
	PruningDisabled bool `json:"pruning_disabled"`

	// PruningFrequency is the frequency (in seconds) that we attempt
	// to prune blocks. If not populated, we use the default value
	// provided in the `statefulsyncer` package.
	PruningFrequency *int `json:"pruning_frequency,omitempty"`

	// InitialBalanceFetchDisabled configures rosetta-cli
	// not to lookup the balance of newly seen accounts at the
	// parent block before applying operations. Disabling
	// this step can significantly speed up performance
	// without impacting validation accuracy (if all genesis
	// accounts are provided using bootstrap_balances and
	// syncing starts from genesis).
	InitialBalanceFetchDisabled bool `json:"initial_balance_fetch_disabled"`

	// ReconcilerActiveBacklog is the maximum number of pending changes
	// to keep in the active reconciliation backlog before skipping
	// reconciliation on new changes.
	ReconcilerActiveBacklog *int `json:"reconciler_active_backlog,omitempty"`
}

// Configuration contains all configuration settings for running
// check:data or check:construction.
type Configuration struct {
	// Network is the *types.NetworkIdentifier where transactions should
	// be constructed and where blocks should be synced to monitor
	// for broadcast success.
	Network *types.NetworkIdentifier `json:"network"`

	// OnlineURL is the URL of a Rosetta API implementation in "online mode".
	OnlineURL string `json:"online_url"`

	// DataDirectory is a folder used to store logs and any data used to perform validation.
	// The path can be absolute, or it can be relative to where rosetta-cli
	// binary is being executed.
	DataDirectory string `json:"data_directory"`

	// HTTPTimeout is the timeout for a HTTP request in seconds.
	HTTPTimeout uint64 `json:"http_timeout"`

	// MaxRetries is the number of times we will retry an HTTP request. If retry_elapsed_time
	// is also populated, we may stop attempting retries early.
	MaxRetries uint64 `json:"max_retries"`

	// RetryElapsedTime is the total time to spend retrying a HTTP request in seconds.
	RetryElapsedTime uint64 `json:"retry_elapsed_time"`

	// MaxOnlineConnections is the maximum number of open connections that the online
	// fetcher will open.
	MaxOnlineConnections int `json:"max_online_connections"`

	// ForceRetry overrides the default retry handling to retry
	// on all non-200 responses.
	ForceRetry bool `json:"force_retry,omitempty"`

	// MaxSyncConcurrency is the maximum sync concurrency to use while syncing blocks.
	// Sync concurrency is managed automatically by the `syncer` package.
	MaxSyncConcurrency int64 `json:"max_sync_concurrency"`

	// TipDelay dictates how many seconds behind the current time is considered
	// tip. If we are > TipDelay seconds from the last processed block,
	// we are considered to be behind tip.
	TipDelay int64 `json:"tip_delay"`

	// MaxReorgDepth specifies the maximum possible reorg depth of the blockchain
	// being synced. This value is used to determine how aggressively to prune
	// old block data.
	//
	// It is better to be overly cautious here as keeping a few
	// too many blocks around is much better than running into an
	// error caused by missing block data!
	MaxReorgDepth int `json:"max_reorg_depth,omitempty"`

	// LogConfiguration determines if the configuration settings
	// should be printed to the console when a file is loaded.
	LogConfiguration bool `json:"log_configuration"`

	// CompressionDisabled configures the storage layer to not
	// perform data compression before writing to disk. This leads
	// to significantly more on-disk storage usage but can lead
	// to performance gains.
	CompressionDisabled bool `json:"compression_disabled"`

	// MemoryLimitDisabled configures storage to increase memory
	// usage. Enabling this massively increases performance
	// but can use 10s of GBs of RAM, even with pruning enabled.
	MemoryLimitDisabled bool `json:"memory_limit_disabled"`

	// SeenBlockWorkers is the number of goroutines spawned to store
	// seen blocks in storage before we attempt to sequence. If not populated,
	// this value defaults to runtime.NumCPU().
	SeenBlockWorkers int `json:"seen_block_workers,omitempty"`

	// SerialBlockWorkers is the number of goroutines spawned to help
	// with block sequencing (i.e. updating balances, updating coins, etc).
	// If not populated, this value defaults to runtime.NumCPU().
	SerialBlockWorkers int `json:"serial_block_workers,omitempty"`

	Construction *ConstructionConfiguration `json:"construction"`
	Data         *DataConfiguration         `json:"data"`
}
