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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"path"
	"runtime"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/dsl"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/fatih/color"
)

// DefaultDataConfiguration returns the default *DataConfiguration
// for running `check:data`.
func DefaultDataConfiguration() *DataConfiguration {
	return &DataConfiguration{
		ActiveReconciliationConcurrency:   DefaultActiveReconciliationConcurrency,
		InactiveReconciliationConcurrency: DefaultInactiveReconciliationConcurrency,
		InactiveReconciliationFrequency:   DefaultInactiveReconciliationFrequency,
		StatusPort:                        DefaultStatusPort,
	}
}

// DefaultConfiguration returns a *Configuration with the
// EthereumNetwork, DefaultURL, DefaultTimeout,
// DefaultConstructionConfiguration and DefaultDataConfiguration.
func DefaultConfiguration() *Configuration {
	return &Configuration{
		Network:              EthereumNetwork,
		OnlineURL:            DefaultURL,
		MaxOnlineConnections: DefaultMaxOnlineConnections,
		HTTPTimeout:          DefaultTimeout,
		MaxRetries:           DefaultMaxRetries,
		MaxSyncConcurrency:   DefaultMaxSyncConcurrency,
		TipDelay:             DefaultTipDelay,
		MaxReorgDepth:        DefaultMaxReorgDepth,
		Data:                 DefaultDataConfiguration(),
	}
}

func populateConstructionMissingFields(
	constructionConfig *ConstructionConfiguration,
) *ConstructionConfiguration {
	if constructionConfig == nil {
		return nil
	}

	if len(constructionConfig.OfflineURL) == 0 {
		constructionConfig.OfflineURL = DefaultURL
	}

	if constructionConfig.MaxOfflineConnections == 0 {
		constructionConfig.MaxOfflineConnections = DefaultMaxOfflineConnections
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

	if constructionConfig.StatusPort == 0 {
		constructionConfig.StatusPort = DefaultStatusPort
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

	if dataConfig.StatusPort == 0 {
		dataConfig.StatusPort = DefaultStatusPort
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

	if config.MaxRetries == 0 {
		config.MaxRetries = DefaultMaxRetries
	}

	if config.MaxOnlineConnections == 0 {
		config.MaxOnlineConnections = DefaultMaxOnlineConnections
	}

	if config.MaxSyncConcurrency == 0 {
		config.MaxSyncConcurrency = DefaultMaxSyncConcurrency
	}

	if config.TipDelay == 0 {
		config.TipDelay = DefaultTipDelay
	}

	if config.MaxReorgDepth == 0 {
		config.MaxReorgDepth = DefaultMaxReorgDepth
	}

	numCPU := runtime.NumCPU()
	if config.SeenBlockWorkers == 0 {
		config.SeenBlockWorkers = numCPU
	}

	if config.SerialBlockWorkers == 0 {
		config.SerialBlockWorkers = numCPU
	}

	config.Construction = populateConstructionMissingFields(config.Construction)
	config.Data = populateDataMissingFields(config.Data)

	return config
}

func assertConstructionConfiguration(ctx context.Context, config *ConstructionConfiguration) error {
	if config == nil {
		return nil
	}

	if len(config.Workflows) > 0 && len(config.ConstructorDSLFile) > 0 {
		return errors.New("cannot populate both workflows and DSL file path")
	}

	if len(config.Workflows) == 0 && len(config.ConstructorDSLFile) == 0 {
		return errors.New("both workflows and DSL file path are empty")
	}

	// Compile ConstructorDSLFile and save to Workflows
	if len(config.ConstructorDSLFile) > 0 {
		compiledWorkflows, err := dsl.Parse(ctx, config.ConstructorDSLFile)
		if err != nil {
			err.Log()
			return fmt.Errorf("%w: compilation failed", err.Err)
		}

		config.Workflows = compiledWorkflows
	}

	// Parse provided Workflows
	for _, workflow := range config.Workflows {
		if workflow.Name == string(job.CreateAccount) || workflow.Name == string(job.RequestFunds) {
			if workflow.Concurrency != job.ReservedWorkflowConcurrency {
				return fmt.Errorf(
					"reserved workflow %s must have concurrency %d",
					workflow.Name,
					job.ReservedWorkflowConcurrency,
				)
			}
		}
	}

	for _, account := range config.PrefundedAccounts {
		// Checks that privkey is hex encoded
		_, err := hex.DecodeString(account.PrivateKeyHex)
		if err != nil {
			return fmt.Errorf(
				"%w: private key %s is not hex encoded for prefunded account",
				err,
				account.PrivateKeyHex,
			)
		}

		// Checks if valid CurveType
		if err := asserter.CurveType(account.CurveType); err != nil {
			return fmt.Errorf("%w: invalid CurveType for prefunded account", err)
		}

		// Checks if valid AccountIdentifier
		if err := asserter.AccountIdentifier(account.AccountIdentifier); err != nil {
			return fmt.Errorf("Account.Address is missing for prefunded account")
		}

		// Check if valid Currency
		err = asserter.Currency(account.Currency)
		if err != nil {
			return fmt.Errorf("%w: invalid currency for prefunded account", err)
		}
	}

	return nil
}

func assertDataConfiguration(config *DataConfiguration) error { // nolint:gocognit
	if config.StartIndex != nil && *config.StartIndex < 0 {
		return fmt.Errorf("start index %d cannot be negative", *config.StartIndex)
	}

	if !config.ReconciliationDisabled && config.BalanceTrackingDisabled {
		return errors.New("balance tracking must be enabled to perform reconciliation")
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
		coverage := config.EndConditions.ReconciliationCoverage.Coverage
		if coverage < 0 || coverage > 1 {
			return fmt.Errorf("reconciliation coverage %f must be [0.0,1.0]", coverage)
		}

		index := config.EndConditions.ReconciliationCoverage.Index
		if index != nil && *index < 0 {
			return fmt.Errorf("reconciliation coverage height %d must be >= 0", *index)
		}

		accountCount := config.EndConditions.ReconciliationCoverage.AccountCount
		if accountCount != nil && *accountCount < 0 {
			return fmt.Errorf(
				"reconciliation coverage account count %d must be >= 0",
				*accountCount,
			)
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

func assertConfiguration(ctx context.Context, config *Configuration) error {
	if err := asserter.NetworkIdentifier(config.Network); err != nil {
		return fmt.Errorf("%w: invalid network identifier", err)
	}

	if config.SeenBlockWorkers <= 0 {
		return errors.New("seen_block_workers must be > 0")
	}

	if config.SerialBlockWorkers <= 0 {
		return errors.New("serial_block_workers must be > 0")
	}

	if err := assertDataConfiguration(config.Data); err != nil {
		return fmt.Errorf("%w: invalid data configuration", err)
	}

	if err := assertConstructionConfiguration(ctx, config.Construction); err != nil {
		return fmt.Errorf("%w: invalid construction configuration", err)
	}

	return nil
}

// modifyFilePaths modifies a collection of filepaths in a *Configuration
// file to make them relative to the configuration file (this makes it a lot easier
// to store all config-related files in the same directory and to run the rosetta-cli
// from a different directory).
func modifyFilePaths(config *Configuration, fileDir string) {
	if config.Data != nil {
		if len(config.Data.BootstrapBalances) > 0 {
			config.Data.BootstrapBalances = path.Join(fileDir, config.Data.BootstrapBalances)
		}

		if len(config.Data.InterestingAccounts) > 0 {
			config.Data.InterestingAccounts = path.Join(fileDir, config.Data.InterestingAccounts)
		}

		if len(config.Data.ExemptAccounts) > 0 {
			config.Data.ExemptAccounts = path.Join(fileDir, config.Data.ExemptAccounts)
		}
	}

	if config.Construction != nil {
		if len(config.Construction.ConstructorDSLFile) > 0 {
			config.Construction.ConstructorDSLFile = path.Join(
				fileDir,
				config.Construction.ConstructorDSLFile,
			)
		}
	}
}

// LoadConfiguration returns a parsed and asserted Configuration for running
// tests.
func LoadConfiguration(ctx context.Context, filePath string) (*Configuration, error) {
	var configRaw Configuration
	if err := utils.LoadAndParse(filePath, &configRaw); err != nil {
		return nil, fmt.Errorf("%w: unable to open configuration file", err)
	}

	config := populateMissingFields(&configRaw)

	// Get the configuration file directory so we can load all files
	// relative to the location of the configuration file.
	fileDir := path.Dir(filePath)
	modifyFilePaths(config, fileDir)

	if err := assertConfiguration(ctx, config); err != nil {
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
