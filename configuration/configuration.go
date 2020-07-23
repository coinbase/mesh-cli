package configuration

import (
	"fmt"
	"log"

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

// ConstructionConfiguration contains all configurations
// to run check:construction.
type ConstructionConfiguration struct {
	Network               *types.NetworkIdentifier `json:"network"`
	OfflineURL            string                   `json:"offline_url"`
	Currency              *types.Currency          `json:"currency"`
	MinimumAccountBalance string                   `json:"minimum_account_balance"`
	MinimumFee            string                   `json:"minimum_fee"`
	MaximumFee            string                   `json:"maximum_fee"`
	CurveType             types.CurveType          `json:"curve_type"`
	AccountingModel       AccountingModel          `json:"accounting_model"`
	TransferScenario      []*types.Operation       `json:"transfer_scenario"`
}

// DataConfiguration contains all configurations to run check:data.
type DataConfiguration struct{} // TODO: Populate and Assert DataConfiguration, Add end conditions, populate defaults that aren't filled in

// Configuration contains all configuration settings for running
// check:data or check:construction.
type Configuration struct {
	OnlineURL    string                     `json:"online_url"`
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
	default:
		return nil, fmt.Errorf("testing more %s not supported", mode)
	}

	log.Printf(
		"loaded configuration file: %s\n",
		types.PrettyPrintStruct(config),
	)

	return &config, nil
}

// TODO: Create Default Configuration File (print out all defaults to the console
// so it is easy for someone to create their own...print out settings for ETH Construction API).
