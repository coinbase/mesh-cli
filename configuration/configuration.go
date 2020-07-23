package configuration

import (
	"fmt"
	"log"

	"github.com/coinbase/rosetta-cli/internal/scenario"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// TestingMode is a type representing possible test types.
// This determines how strictly to parse the configuration file.
type TestingMode string

const (
	// DataMode is testing of the Data API using check:data.
	DataMode TestingMode = "data"

	// ViewMode is viewing certain objects using the Data API.
	ViewMode TestingMode = "view"

	// ConstructionMode is testing of the Construction API using check:construction.
	ConstructionMode TestingMode = "construction"
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

// TODO: Add support for sophisticated end conditions(https://github.com/coinbase/rosetta-cli/issues/66)

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	// TODO: Write comments about each value
	Network               *types.NetworkIdentifier `json:"network"`
	OnlineURL             string                   `json:"online_url"`
	OfflineURL            string                   `json:"offline_url"`
	Currency              *types.Currency          `json:"currency"`
	MinimumAccountBalance string                   `json:"minimum_account_balance"`
	MinimumFee            string                   `json:"minimum_fee"`
	MaximumFee            string                   `json:"maximum_fee"`
	CurveType             types.CurveType          `json:"curve_type"`
	AccountingModel       AccountingModel          `json:"accounting_model"`
	TransferScenario      []*types.Operation       `json:"transfer_scenario"`
}

func DefaultConstructionConfiguration() *ConstructionConfiguration {
	// DEFAULT IS ETH
	return &ConstructionConfiguration{
		Network: &types.NetworkIdentifier{
			Blockchain: "Ethereum",
			Network:    "Ropsten",
		},
		OnlineURL:  "http://localhost:8080",
		OfflineURL: "http://localhost:8080",
		Currency: &types.Currency{
			Symbol:   "ETH",
			Decimals: 18,
		},
		MinimumAccountBalance: "0",
		MinimumFee:            "21000",
		MaximumFee:            "10000000",
		CurveType:             types.Secp256k1,
		AccountingModel:       AccountModel,
		TransferScenario: []*types.Operation{
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
		},
	}
}

func DefaultDataConfiguration() *DataConfiguration {
	return &DataConfiguration{
		OnlineURL:                         "http://localhost:8080",
		BlockConcurrency:                  8,
		TransactionConcurrency:            16,
		ActiveReconciliationConcurrency:   16,
		InactiveReconciliationConcurrency: 4,
		InactiveReconciliationFrequency:   250,
		HaltOnReconciliationError:         true,
		LookupBalanceByBlock:              true, // override with setting returned by node?
	}
}

func DefaultConfiguration() *Configuration {
	return &Configuration{
		Construction: DefaultConstructionConfiguration(),
		Data:         DefaultDataConfiguration(),
	}
}

// DataConfiguration contains all configurations to run check:data.
// TODO: Add timeout!!
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

	// HaltOnReconciliationError determines if block processing should halt on a reconciliation
	// error. It can be beneficial to collect all reconciliation errors or silence
	// reconciliation errors during development.
	// default: true
	HaltOnReconciliationError bool `json:"halt_on_reconciliation_error"`

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

	// LookupBalanceByBlock is a boolean that dictates how balance lookup is performed.
	// When set to true, balances are looked up at the block where a balance
	// change occurred instead of at the current block. Blockchains that do not support
	// historical balance lookup should set this to false.
	// default: true
	LookupBalanceByBlock bool `json:"lookup_balance_by_block"`

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

func assertConstructionConfiguration(config *ConstructionConfiguration) error {
	switch config.AccountingModel {
	case AccountModel, UtxoModel:
	default:
		return fmt.Errorf("accounting model %s not supported", config.AccountingModel)
	}

	if err := asserter.CurveType(config.CurveType); err != nil {
		return fmt.Errorf("%w: invalid curve type", err)
	}

	return nil
}

func assertDataConfiguration(config *DataConfiguration) error {
	return fmt.Errorf("not implemented")
}

// LoadConfiguration returns a parsed and asserted Configuration for running
// tests.
func LoadConfiguration(filePath string, mode TestingMode) (*Configuration, error) {
	var config Configuration
	if err := utils.LoadAndParse(filePath, &config); err != nil {
		return nil, fmt.Errorf("%w: unable to open configuration file", err)
	}

	switch mode {
	case ConstructionMode:
		if err := assertConstructionConfiguration(config.Construction); err != nil {
			return nil, fmt.Errorf("%w: invalid construction configuration", err)
		}
	case DataMode, ViewMode:
		if err := assertDataConfiguration(config.Data); err != nil {
			return nil, fmt.Errorf("%w: invalid data configuration", err)
		}
	default:
		return nil, fmt.Errorf("testing more %s not supported", mode)
	}

	log.Printf(
		"loaded configuration file: %s\n",
		types.PrettyPrintStruct(config),
	)

	// TODO: Populate defaults

	return &config, nil
}

// TODO: Create Default Configuration File (print out all defaults to the console
// so it is easy for someone to create their own...print out settings for ETH Construction API).
