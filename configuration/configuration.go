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
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
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
	DefaultSyncConcurrency                   = 8
	DefaultTransactionConcurrency            = 16
	DefaultActiveReconciliationConcurrency   = 16
	DefaultInactiveReconciliationConcurrency = 4
	DefaultInactiveReconciliationFrequency   = 250
	DefaultTimeout                           = 10
	DefaultRetryElapsedTime                  = 60
	DefaultConfirmationDepth                 = 10
	DefaultStaleDepth                        = 30
	DefaultBroadcastLimit                    = 3
	DefaultTipDelay                          = 300
	DefaultBlockBroadcastLimit               = 5

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

// TODO: Add support for sophisticated end conditions
// (https://github.com/coinbase/rosetta-cli/issues/66)

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	// OfflineURL is the URL of a Rosetta API implementation in "offline mode".
	OfflineURL string `json:"offline_url"`

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
	PrefundedAccounts []*storage.PrefundedAccount `json:"prefunded_accounts"`

	// Workflows are executed by the rosetta-cli to test
	// certain construction flows. Make sure to define a
	// "request_funds" and "create_account" workflow.
	Workflows []*job.Workflow `json:"workflows"`
}

// DefaultConstructionConfiguration returns the *ConstructionConfiguration
// used for testing Ethereum transfers on Ropsten.
func DefaultConstructionConfiguration() *ConstructionConfiguration {
	return &ConstructionConfiguration{
		OfflineURL:          DefaultURL,
		StaleDepth:          DefaultStaleDepth,
		BroadcastLimit:      DefaultBroadcastLimit,
		BlockBroadcastLimit: DefaultBlockBroadcastLimit,
	}
}

// DefaultDataConfiguration returns the default *DataConfiguration
// for running `check:data`.
func DefaultDataConfiguration() *DataConfiguration {
	return &DataConfiguration{
		ActiveReconciliationConcurrency:   DefaultActiveReconciliationConcurrency,
		InactiveReconciliationConcurrency: DefaultInactiveReconciliationConcurrency,
		InactiveReconciliationFrequency:   DefaultInactiveReconciliationFrequency,
	}
}

// DefaultConfiguration returns a *Configuration with the
// EthereumNetwork, DefaultURL, DefaultTimeout,
// DefaultConstructionConfiguration and DefaultDataConfiguration.
func DefaultConfiguration() *Configuration {
	return &Configuration{
		Network:                EthereumNetwork,
		OnlineURL:              DefaultURL,
		HTTPTimeout:            DefaultTimeout,
		RetryElapsedTime:       DefaultRetryElapsedTime,
		SyncConcurrency:        DefaultSyncConcurrency,
		TransactionConcurrency: DefaultTransactionConcurrency,
		TipDelay:               DefaultTipDelay,
		Construction:           DefaultConstructionConfiguration(),
		Data:                   DefaultDataConfiguration(),
	}
}

// DataEndConditions contains all the conditions for the syncer to stop
// when running check:data.
// Only 1 end condition can be populated at once!
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

	// ReconciliationCoverage configures the syncer to stop
	// once it has reached tip AND some proportion of
	// all addresses have been reconciled at an index >=
	// to when tip was first reached. The range of inputs
	// for this condition are [0.0, 1.0].
	ReconciliationCoverage *float64 `json:"reconciliation_coverage,omitempty"`
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

	// ExemptAccounts is a path to a file listing all accounts to exempt from balance
	// tracking and reconciliation. Look at the examples directory for an example of
	// how to structure this file.
	ExemptAccounts string `json:"exempt_accounts"`

	// BootstrapBalances is a path to a file used to bootstrap balances
	// before starting syncing. If this value is populated after beginning syncing,
	// it will be ignored.
	BootstrapBalances string `json:"bootstrap_balances"`

	// HistoricalBalanceDisabled is a boolean that dictates how balance lookup is performed.
	// When set to true, balances are looked up at the block where a balance
	// change occurred instead of at the current block. Blockchains that do not support
	// historical balance lookup should set this to false.
	HistoricalBalanceDisabled bool `json:"historical_balance_disabled"`

	// InterestingAccounts is a path to a file listing all accounts to check on each block. Look
	// at the examples directory for an example of how to structure this file.
	InterestingAccounts string `json:"interesting_accounts"`

	// ReconciliationDisabled is a boolean that indicates reconciliation should not
	// be attempted. When first testing an implementation, it can be useful to disable
	// some of the more advanced checks to confirm syncing is working as expected.
	ReconciliationDisabled bool `json:"reconciliation_disabled"`

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

	// ResultsOutputFile is the absolute filepath of where to save
	// the results of a check:data run.
	ResultsOutputFile string `json:"results_output_file"`
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
	DataDirectory string `json:"data_directory"`

	// HTTPTimeout is the timeout for a HTTP request in seconds.
	HTTPTimeout uint64 `json:"http_timeout"`

	// RetryElapsedTime is the total time to spend retrying a HTTP request in seconds.
	RetryElapsedTime uint64 `json:"retry_elapsed_time"`

	// SyncConcurrency is the concurrency to use while syncing blocks.
	SyncConcurrency uint64 `json:"sync_concurrency"`

	// TransactionConcurrency is the concurrency to use while fetching transactions (if required).
	TransactionConcurrency uint64 `json:"transaction_concurrency"`

	// TipDelay dictates how many seconds behind the current time is considered
	// tip. If we are > TipDelay seconds from the last processed block,
	// we are considered to be behind tip.
	TipDelay int64 `json:"tip_delay"`

	// DisableMemoryLimit uses a performance-optimized database mode
	// that uses more memory.
	DisableMemoryLimit bool `json:"disable_memory_limit"`

	// LogConfiguration determines if the configuration settings
	// should be printed to the console when a file is loaded.
	LogConfiguration bool `json:"log_configuration"`

	Construction *ConstructionConfiguration `json:"construction"`
	Data         *DataConfiguration         `json:"data"`
}

func populateConstructionMissingFields(
	constructionConfig *ConstructionConfiguration,
) *ConstructionConfiguration {
	if constructionConfig == nil {
		return DefaultConstructionConfiguration()
	}

	if len(constructionConfig.OfflineURL) == 0 {
		constructionConfig.OfflineURL = DefaultURL
	}

	if constructionConfig.StaleDepth == 0 {
		constructionConfig.StaleDepth = DefaultStaleDepth
	}

	if constructionConfig.BroadcastLimit == 0 {
		constructionConfig.BroadcastLimit = DefaultBroadcastLimit
	}

	if constructionConfig.BlockBroadcastLimit == 0 {
		constructionConfig.BlockBroadcastLimit = DefaultBlockBroadcastLimit
	}

	return constructionConfig
}

func populateDataMissingFields(dataConfig *DataConfiguration) *DataConfiguration {
	if dataConfig == nil {
		return DefaultDataConfiguration()
	}

	if dataConfig.ActiveReconciliationConcurrency == 0 {
		dataConfig.ActiveReconciliationConcurrency = DefaultActiveReconciliationConcurrency
	}

	if dataConfig.InactiveReconciliationConcurrency == 0 {
		dataConfig.InactiveReconciliationConcurrency = DefaultInactiveReconciliationConcurrency
	}

	if dataConfig.InactiveReconciliationFrequency == 0 {
		dataConfig.InactiveReconciliationFrequency = DefaultInactiveReconciliationFrequency
	}

	return dataConfig
}

func populateMissingFields(config *Configuration) *Configuration {
	if config == nil {
		return DefaultConfiguration()
	}

	if config.Network == nil {
		config.Network = EthereumNetwork
	}

	if len(config.OnlineURL) == 0 {
		config.OnlineURL = DefaultURL
	}

	if config.HTTPTimeout == 0 {
		config.HTTPTimeout = DefaultTimeout
	}

	if config.RetryElapsedTime == 0 {
		config.RetryElapsedTime = DefaultRetryElapsedTime
	}

	if config.SyncConcurrency == 0 {
		config.SyncConcurrency = DefaultSyncConcurrency
	}

	if config.TransactionConcurrency == 0 {
		config.TransactionConcurrency = DefaultTransactionConcurrency
	}

	if config.TipDelay == 0 {
		config.TipDelay = DefaultTipDelay
	}

	config.Construction = populateConstructionMissingFields(config.Construction)
	config.Data = populateDataMissingFields(config.Data)

	return config
}

func checkStringUint(input string) error {
	val, ok := new(big.Int).SetString(input, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", input)
	}

	if val.Sign() == -1 {
		return fmt.Errorf("%s must not be negative", input)
	}

	return nil
}

func assertConstructionConfiguration(config *ConstructionConfiguration) error {
	for _, account := range config.PrefundedAccounts {
		// Checks that privkey is hex encoded
		_, err := hex.DecodeString(account.PrivateKeyHex)
		if err != nil {
			return fmt.Errorf("%s is not hex encoded", account.PrivateKeyHex)
		}

		// Checks if valid curvetype
		err = asserter.CurveType(account.CurveType)
		if err != nil {
			return fmt.Errorf("invalid CurveType %s", err)
		}

		// Checks if address is not empty string
		if account.Address == "" {
			return fmt.Errorf("Account.Address is missing")
		}
	}

	return nil
}

func assertDataConfiguration(config *DataConfiguration) error {
	if config.StartIndex != nil && *config.StartIndex < 0 {
		return fmt.Errorf("start index %d cannot be negative", *config.StartIndex)
	}

	if config.EndConditions == nil {
		return nil
	}

	if config.EndConditions.Index != nil {
		if *config.EndConditions.Index < 0 {
			return fmt.Errorf("end index %d cannot be negative", *config.EndConditions.Index)
		}
	}

	if config.EndConditions.ReconciliationCoverage != nil {
		coverage := *config.EndConditions.ReconciliationCoverage
		if coverage < 0 || coverage > 1 {
			return fmt.Errorf("reconciliation coverage %f must be [0.0,1.0]", coverage)
		}

		if config.BalanceTrackingDisabled {
			return errors.New(
				"balance tracking must be enabled for reconciliation coverage end condition",
			)
		}

		if config.IgnoreReconciliationError {
			return errors.New(
				"reconciliation errors cannot be ignored for reconciliation coverage end condition",
			)
		}

		if config.ReconciliationDisabled {
			return errors.New(
				"reconciliation cannot be disabled for reconciliation coverage end condition",
			)
		}
	}

	return nil
}

func assertConfiguration(config *Configuration) error {
	if err := asserter.NetworkIdentifier(config.Network); err != nil {
		return fmt.Errorf("%w: invalid network identifier", err)
	}

	if err := assertDataConfiguration(config.Data); err != nil {
		return fmt.Errorf("%w: invalid data configuration", err)
	}

	if err := assertConstructionConfiguration(config.Construction); err != nil {
		return fmt.Errorf("%w: invalid construction configuration", err)
	}

	return nil
}

// LoadConfiguration returns a parsed and asserted Configuration for running
// tests.
func LoadConfiguration(filePath string) (*Configuration, error) {
	var configRaw Configuration
	if err := utils.LoadAndParse(filePath, &configRaw); err != nil {
		return nil, fmt.Errorf("%w: unable to open configuration file", err)
	}

	config := populateMissingFields(&configRaw)

	if err := assertConfiguration(config); err != nil {
		return nil, fmt.Errorf("%w: invalid configuration", err)
	}

	color.Cyan(
		"loaded configuration file: %s\n",
		filePath,
	)

	if config.LogConfiguration {
		log.Println(types.PrettyPrintStruct(config))
	}

	return config, nil
}
