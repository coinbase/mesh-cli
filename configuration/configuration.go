package configuration

import (
	"fmt"
	"log"
	"math/big"

	"github.com/coinbase/rosetta-cli/internal/scenario"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
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
	DefaultMinimumBalance                    = "0"
	DefaultMaximumFee                        = "50000"
	DefaultCurveType                         = types.Secp256k1
	DefaultAccountingModel                   = AccountModel
	DefaultBlockConcurrency                  = 8
	DefaultTransactionConcurrency            = 16
	DefaultActiveReconciliationConcurrency   = 16
	DefaultInactiveReconciliationConcurrency = 4
	DefaultInactiveReconciliationFrequency   = 250
)

// Default Configuration Values
var (
	DefaultNetwork = &types.NetworkIdentifier{
		Blockchain: "Ethereum",
		Network:    "Ropsten",
	}
	DefaultCurrency = &types.Currency{
		Symbol:   "ETH",
		Decimals: 18,
	}
	DefaultTransferScenario = []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Account: &types.AccountIdentifier{
				Address: scenario.Sender,
			},
			Type: "transfer",
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
			Type: "transfer",
			Amount: &types.Amount{
				Value: scenario.RecipientValue,
			},
		},
	}
)

// TODO: Add support for sophisticated end conditions (https://github.com/coinbase/rosetta-cli/issues/66)

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	// Network is the *types.NetworkIdentifier where transactions should
	// be constructed and where blocks should be synced to monitor
	// for broadcast success.
	Network *types.NetworkIdentifier `json:"network"`

	// OnlineURL is the URL of a Rosetta API implementation in "online mode".
	// default: http://localhost:8080
	OnlineURL string `json:"online_url"`

	// OfflineURL is the URL of a Rosetta API implementation in "online mode".
	// default: http://localhost:8080
	OfflineURL string `json:"offline_url"`

	// Currency is the *types.Currency to track and use for transactions.
	// default: {Symbol: "ETH", Decimals: 18}
	Currency *types.Currency `json:"currency"`

	// MinimumBalance is balance at a particular address
	// that is not considered spendable.
	// default: "0"
	MinimumBalance string `json:"minimum_balance"`

	// MaximumFee is the maximum fee that could be used
	// to send a transaction. The sendable balance
	// of any address is calculated as balance - minimum_balance - maximum_fee.
	// default: "10000000"
	MaximumFee string `json:"maximum_fee"`

	// CurveType is the curve to use when generating a *keys.KeyPair.
	// default: "secp256k1"
	CurveType types.CurveType `json:"curve_type"`

	// AccountingModel is the type of acccount model to use for
	// testing (account vs UTXO).
	// default: "account"
	AccountingModel AccountingModel `json:"accounting_model"`

	// TransferScenario contains a slice of operations that
	// indicate how to perform a transfer on a blockchain. In the future
	// this will be expanded to support all kinds of construction scenarios (like
	// staking or governance).
	// default: ETH transfer
	TransferScenario []*types.Operation `json:"transfer_scenario"`
}

// DefaultConstructionConfiguration returns the *ConstructionConfiguration
// used for testing Ethereum transfers on Ropsten.
func DefaultConstructionConfiguration() *ConstructionConfiguration {
	return &ConstructionConfiguration{
		Network:          DefaultNetwork,
		OnlineURL:        DefaultURL,
		OfflineURL:       DefaultURL,
		Currency:         DefaultCurrency,
		MinimumBalance:   DefaultMinimumBalance,
		MaximumFee:       DefaultMaximumFee,
		CurveType:        DefaultCurveType,
		AccountingModel:  DefaultAccountingModel,
		TransferScenario: DefaultTransferScenario,
	}
}

// DefaultDataConfiguration returns the default *DataConfiguration
// for running `check:data`.
func DefaultDataConfiguration() *DataConfiguration {
	return &DataConfiguration{
		OnlineURL:                         DefaultURL,
		BlockConcurrency:                  DefaultBlockConcurrency,
		TransactionConcurrency:            DefaultTransactionConcurrency,
		ActiveReconciliationConcurrency:   DefaultActiveReconciliationConcurrency,
		InactiveReconciliationConcurrency: DefaultInactiveReconciliationConcurrency,
		InactiveReconciliationFrequency:   DefaultInactiveReconciliationFrequency,
	}
}

// DefaultConfiguration returns a *Configuration with the
// DefaultConstructionConfiguration and DefaultDataConfiguration.
func DefaultConfiguration() *Configuration {
	return &Configuration{
		Construction: DefaultConstructionConfiguration(),
		Data:         DefaultDataConfiguration(),
	}
}

// DataConfiguration contains all configurations to run check:data.
// TODO: Add configurable timeout (https://github.com/coinbase/rosetta-cli/issues/64)
type DataConfiguration struct {
	// OnlineURL is the URL of a Rosetta API implementation in "online mode".
	// default: http://localhost:8080
	OnlineURL string `json:"online_url"`

	// DataDirectory is a folder used to store logs and any data used to perform validation.
	// default: ""
	DataDirectory string `json:"data_directory"`

	// BlockConcurrency is the concurrency to use while fetching blocks.
	// default: 8
	BlockConcurrency uint64 `json:"block_concurrency"`

	// TransactionConcurrency is the concurrency to use while fetching transactions (if required).
	// default: 16
	TransactionConcurrency uint64 `json:"transaction_concurrency"`

	// ActiveReconciliationConcurrency is the concurrency to use while fetching accounts
	// during active reconciliation.
	// default: 8
	ActiveReconciliationConcurrency uint64 `json:"active_reconciliation_concurrency"`

	// InactiveReconciliationConcurrency is the concurrency to use while fetching accounts
	// during inactive reconciliation.
	// default: 4
	InactiveReconciliationConcurrency uint64 `json:"inactive_reconciliation_concurrency"`

	// InactiveReconciliationFrequency is the number of blocks to wait between
	// inactive reconiliations on each account.
	// default: 250
	InactiveReconciliationFrequency uint64 `json:"inactive_reconciliation_frequency"`

	// LogBlocks is a boolean indicating whether to log processed blocks.
	// default: false
	LogBlocks bool `json:"log_blocks"`

	// LogTransactions is a boolean indicating whether to log processed transactions.
	// default: false
	LogTransactions bool `json:"log_transactions"`

	// LogBalanceChanges is a boolean indicating whether to log all balance changes.
	// default: false
	LogBalanceChanges bool `json:"log_balance_changes"`

	// LogReconciliations is a boolean indicating whether to log all reconciliations.
	// default: false
	LogReconciliations bool `json:"log_reconciliations"`

	// IgnoreReconciliationError determines if block processing should halt on a reconciliation
	// error. It can be beneficial to collect all reconciliation errors or silence
	// reconciliation errors during development.
	// default: false
	IgnoreReconciliationError bool `json:"ignore_reconciliation_error"`

	// ExemptAccounts is a path to a file listing all accounts to exempt from balance
	// tracking and reconciliation. Look at the examples directory for an example of
	// how to structure this file.
	// default: ""
	ExemptAccounts string `json:"exempt_accounts"`

	// BootstrapBalances is a path to a file used to bootstrap balances
	// before starting syncing. If this value is populated after beginning syncing,
	// it will be ignored.
	// default: ""
	BootstrapBalances string `json:"bootstrap_balances"`

	// HistoricalBalanceDisabled is a boolean that dictates how balance lookup is performed.
	// When set to true, balances are looked up at the block where a balance
	// change occurred instead of at the current block. Blockchains that do not support
	// historical balance lookup should set this to false.
	// default: false
	HistoricalBalanceDisabled bool `json:"historical_balance_disabled"`

	// InterestingAccounts is a path to a file listing all accounts to check on each block. Look
	// at the examples directory for an example of how to structure this file.
	// default: ""
	InterestingAccounts string `json:"interesting_accounts"`
}

// Configuration contains all configuration settings for running
// check:data or check:construction.
type Configuration struct {
	Construction *ConstructionConfiguration `json:"construction"`
	Data         *DataConfiguration         `json:"data"`
}

func populateConstructionMissingFields(constructionConfig *ConstructionConfiguration) *ConstructionConfiguration {
	if constructionConfig == nil {
		return DefaultConstructionConfiguration()
	}

	if constructionConfig.Network == nil {
		constructionConfig.Network = DefaultNetwork
	}

	if len(constructionConfig.OnlineURL) == 0 {
		constructionConfig.OnlineURL = DefaultURL
	}

	if len(constructionConfig.OfflineURL) == 0 {
		constructionConfig.OfflineURL = DefaultURL
	}

	if constructionConfig.Currency == nil {
		constructionConfig.Currency = DefaultCurrency
	}

	if len(constructionConfig.MinimumBalance) == 0 {
		constructionConfig.MinimumBalance = DefaultMinimumBalance
	}

	if len(constructionConfig.MaximumFee) == 0 {
		constructionConfig.MaximumFee = DefaultMaximumFee
	}

	if len(constructionConfig.CurveType) == 0 {
		constructionConfig.CurveType = DefaultCurveType
	}

	if len(constructionConfig.AccountingModel) == 0 {
		constructionConfig.AccountingModel = DefaultAccountingModel
	}

	if len(constructionConfig.TransferScenario) == 0 {
		constructionConfig.TransferScenario = DefaultTransferScenario
	}

	return constructionConfig
}

func populateDataMissingFields(dataConfig *DataConfiguration) *DataConfiguration {
	if dataConfig == nil {
		return DefaultDataConfiguration()
	}

	if len(dataConfig.OnlineURL) == 0 {
		dataConfig.OnlineURL = DefaultURL
	}

	if dataConfig.BlockConcurrency == 0 {
		dataConfig.BlockConcurrency = DefaultBlockConcurrency
	}

	if dataConfig.TransactionConcurrency == 0 {
		dataConfig.TransactionConcurrency = DefaultTransactionConcurrency
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

func populateMissingFields(config *Configuration) {
	if config == nil {
		config = DefaultConfiguration()
		return
	}

	config.Construction = populateConstructionMissingFields(config.Construction)
	config.Data = populateDataMissingFields(config.Data)

	return
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
	if err := asserter.NetworkIdentifier(config.Network); err != nil {
		return fmt.Errorf("%w: invalid network identifier", err)
	}

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

// LoadConfiguration returns a parsed and asserted Configuration for running
// tests.
func LoadConfiguration(filePath string) (*Configuration, error) {
	var config Configuration
	if err := utils.LoadAndParse(filePath, &config); err != nil {
		return nil, fmt.Errorf("%w: unable to open configuration file", err)
	}

	populateMissingFields(&config)

	if err := assertConstructionConfiguration(config.Construction); err != nil {
		return nil, fmt.Errorf("%w: invalid construction configuration", err)
	}

	log.Printf(
		"loaded configuration: %s\n",
		types.PrettyPrintStruct(config),
	)

	return &config, nil
}
