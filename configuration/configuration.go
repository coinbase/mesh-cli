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
	"fmt"
	"log"
	"math/big"

	"github.com/slowboat0/rosetta-cli/pkg/scenario"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
)

// AccountingModel is a type representing possible accounting models
// in the Construction API.
type AccountingModel string

const (
	// AccountModel is for account-based blockchains.
	AccountModel AccountingModel = "account"

	// UtxoModel is for UTXO-based blockchains.
	UtxoModel AccountingModel = "utxo"
)

// Default Configuration Values
const (
	DefaultURL                               = "http://localhost:8080"
	DefaultBlockConcurrency                  = 8
	DefaultTransactionConcurrency            = 16
	DefaultActiveReconciliationConcurrency   = 16
	DefaultInactiveReconciliationConcurrency = 4
	DefaultInactiveReconciliationFrequency   = 250
	DefaultTimeout                           = 10
	DefaultConfirmationDepth                 = 10
	DefaultStaleDepth                        = 30
	DefaultBroadcastLimit                    = 3
	DefaultTipDelay                          = 300
	DefaultBlockBroadcastLimit               = 5
	DefaultNewAccountProbability             = 0.5
	DefaultMaxAddresses                      = 200

	// ETH Defaults
	EthereumIDBlockchain    = "Ethereum"
	EthereumIDNetwork       = "Ropsten"
	EthereumTransferType    = "transfer"
	EthereumSymbol          = "ETH"
	EthereumDecimals        = 18
	EthereumMinimumBalance  = "0"
	EthereumMaximumFee      = "5000000000000000" // 0.005 ETH
	EthereumCurveType       = types.Secp256k1
	EthereumAccountingModel = AccountModel
)

// Default Configuration Values
var (
	EthereumNetwork = &types.NetworkIdentifier{
		Blockchain: EthereumIDBlockchain,
		Network:    EthereumIDNetwork,
	}
	EthereumCurrency = &types.Currency{
		Symbol:   EthereumSymbol,
		Decimals: EthereumDecimals,
	}
	EthereumTransfer = []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Account: &types.AccountIdentifier{
				Address: scenario.Sender,
			},
			Type: EthereumTransferType,
			Amount: &types.Amount{
				Value: scenario.SenderValue,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			RelatedOperations: []*types.OperationIdentifier{
				{
					Index: 0,
				},
			},
			Account: &types.AccountIdentifier{
				Address: scenario.Recipient,
			},
			Type: EthereumTransferType,
			Amount: &types.Amount{
				Value: scenario.RecipientValue,
			},
		},
	}
)

// TODO: Add support for sophisticated end conditions
// (https://github.com/coinbase/rosetta-cli/issues/66)

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	// OfflineURL is the URL of a Rosetta API implementation in "online mode".
	OfflineURL string `json:"offline_url"`

	// Currency is the *types.Currency to track and use for transactions.
	Currency *types.Currency `json:"currency"`

	// MinimumBalance is balance at a particular address
	// that is not considered spendable.
	MinimumBalance string `json:"minimum_balance"`

	// MaximumFee is the maximum fee that could be used
	// to send a transaction. The sendable balance
	// of any address is calculated as balance - minimum_balance - maximum_fee.
	MaximumFee string `json:"maximum_fee"`

	// CurveType is the curve to use when generating a *keys.KeyPair.
	CurveType types.CurveType `json:"curve_type"`

	// AccountingModel is the type of acccount model to use for
	// testing (account vs UTXO).
	AccountingModel AccountingModel `json:"accounting_model"`

	// Scenario contains a slice of operations that
	// indicate how to construct a transaction on a blockchain. In the future
	// this will be expanded to support all kinds of construction scenarios (like
	// staking or governance).
	Scenario []*types.Operation `json:"scenario"`

	// ConfirmationDepth is the number of blocks that must be synced
	// after a transaction before a transaction is confirmed.
	//
	// Note: Rosetta uses Bitcoin's definition of depth, so
	// a transaction has depth 1 if it is in the head block.
	// Source: https://en.bitcoin.it/wiki/Confirmation
	ConfirmationDepth int64 `json:"confirmation_depth"`

	// StaleDepth is the number of blocks to wait before attempting
	// to rebroadcast after not finding a transaction on-chain.
	StaleDepth int64 `json:"stale_depth"`

	// BroadcastLimit is the number of times to attempt re-broadcast
	// before giving up on a transaction broadcast.
	BroadcastLimit int `json:"broadcast_limit"`

	// IgnoreBroadcastFailures determines if we should exit when there
	// are broadcast failures (that surpass the BroadcastLimit).
	IgnoreBroadcastFailures bool `json:"ignore_broadcast_failures"`

	// ChangeScenario is added to the scenario if it is possible to generate
	// a change transaction where the recipient address is over
	// the minimum balance. If this is left nil, no change
	// will ever be created. This is ONLY used for UTXO-based
	// testing.
	ChangeScenario *types.Operation `json:"change_scenario"`

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

	// NewAccountProbability is the probability we create a new
	// recipient address on any transaction creation loop.
	NewAccountProbability float64 `json:"new_account_probability"`

	// MaxAddresses is the maximum number of addresses
	// to generate while testing.
	MaxAddresses int `json:"max_addresses"`
}

// DefaultConstructionConfiguration returns the *ConstructionConfiguration
// used for testing Ethereum transfers on Ropsten.
func DefaultConstructionConfiguration() *ConstructionConfiguration {
	return &ConstructionConfiguration{
		OfflineURL:            DefaultURL,
		Currency:              EthereumCurrency,
		MinimumBalance:        EthereumMinimumBalance,
		MaximumFee:            EthereumMaximumFee,
		CurveType:             EthereumCurveType,
		AccountingModel:       EthereumAccountingModel,
		Scenario:              EthereumTransfer,
		ConfirmationDepth:     DefaultConfirmationDepth,
		StaleDepth:            DefaultStaleDepth,
		BroadcastLimit:        DefaultBroadcastLimit,
		BlockBroadcastLimit:   DefaultBlockBroadcastLimit,
		NewAccountProbability: DefaultNewAccountProbability,
		MaxAddresses:          DefaultMaxAddresses,
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
		BlockConcurrency:       DefaultBlockConcurrency,
		TransactionConcurrency: DefaultTransactionConcurrency,
		TipDelay:               DefaultTipDelay,
		Construction:           DefaultConstructionConfiguration(),
		Data:                   DefaultDataConfiguration(),
	}
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

	// HTTPTimeout is the timeout for HTTP requests in seconds.
	HTTPTimeout uint64 `json:"http_timeout"`

	// BlockConcurrency is the concurrency to use while fetching blocks.
	BlockConcurrency uint64 `json:"block_concurrency"`

	// TransactionConcurrency is the concurrency to use while fetching transactions (if required).
	TransactionConcurrency uint64 `json:"transaction_concurrency"`

	// TipDelay dictates how many seconds behind the current time is considered
	// tip. If we are > TipDelay seconds from the last processed block,
	// we are considered to be behind tip.
	TipDelay int64 `json:"tip_delay"`

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

	if constructionConfig.Currency == nil {
		constructionConfig.Currency = EthereumCurrency
	}

	if len(constructionConfig.MinimumBalance) == 0 {
		constructionConfig.MinimumBalance = EthereumMinimumBalance
	}

	if len(constructionConfig.MaximumFee) == 0 {
		constructionConfig.MaximumFee = EthereumMaximumFee
	}

	if len(constructionConfig.CurveType) == 0 {
		constructionConfig.CurveType = EthereumCurveType
	}

	if len(constructionConfig.AccountingModel) == 0 {
		constructionConfig.AccountingModel = EthereumAccountingModel
	}

	if len(constructionConfig.Scenario) == 0 {
		constructionConfig.Scenario = EthereumTransfer
	}

	if constructionConfig.ConfirmationDepth == 0 {
		constructionConfig.ConfirmationDepth = DefaultConfirmationDepth
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

	if constructionConfig.NewAccountProbability == 0 {
		constructionConfig.NewAccountProbability = DefaultNewAccountProbability
	}

	if constructionConfig.MaxAddresses == 0 {
		constructionConfig.MaxAddresses = DefaultMaxAddresses
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

	if config.BlockConcurrency == 0 {
		config.BlockConcurrency = DefaultBlockConcurrency
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
	// TODO: add asserter.Currency method
	if err := asserter.Amount(&types.Amount{Value: "0", Currency: config.Currency}); err != nil {
		return fmt.Errorf("%w: invalid currency", err)
	}

	switch config.AccountingModel {
	case AccountModel, UtxoModel:
	default:
		return fmt.Errorf("accounting model %s not supported", config.AccountingModel)
	}

	if err := asserter.CurveType(config.CurveType); err != nil {
		return fmt.Errorf("%w: invalid curve type", err)
	}

	if err := checkStringUint(config.MinimumBalance); err != nil {
		return fmt.Errorf("%w: invalid value for MinimumBalance", err)
	}

	if err := checkStringUint(config.MaximumFee); err != nil {
		return fmt.Errorf("%w: invalid value for MaximumFee", err)
	}

	return nil
}

func assertConfiguration(config *Configuration) error {
	if err := asserter.NetworkIdentifier(config.Network); err != nil {
		return fmt.Errorf("%w: invalid network identifier", err)
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

	log.Println(types.PrettyPrintStruct(config))

	return config, nil
}
