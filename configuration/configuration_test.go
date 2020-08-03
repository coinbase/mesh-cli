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
	"io/ioutil"
	"os"
	"testing"

	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	whackyConfig = &Configuration{
		Network: &types.NetworkIdentifier{
			Blockchain: "sweet",
			Network:    "sweeter",
		},
		OnlineURL:              "http://hasudhasjkdk",
		HTTPTimeout:            21,
		BlockConcurrency:       12,
		TransactionConcurrency: 2,
		TipDelay:               1231,
		Construction: &ConstructionConfiguration{
			OfflineURL: "https://ashdjaksdkjshdk",
			Currency: &types.Currency{
				Symbol:   "FIRE",
				Decimals: 100,
			},
			MinimumBalance:        "1002",
			MaximumFee:            "1",
			CurveType:             types.Edwards25519,
			AccountingModel:       UtxoModel,
			Scenario:              EthereumTransfer,
			ChangeScenario:        EthereumTransfer[0],
			ConfirmationDepth:     100,
			StaleDepth:            12,
			BroadcastLimit:        200,
			BlockBroadcastLimit:   992,
			NewAccountProbability: 0.1,
			MaxAddresses:          12,
		},
		Data: &DataConfiguration{
			ActiveReconciliationConcurrency:   100,
			InactiveReconciliationConcurrency: 2938,
			InactiveReconciliationFrequency:   3,
			ReconciliationDisabled:            true,
			HistoricalBalanceDisabled:         true,
		},
	}
	invalidNetwork = &Configuration{
		Network: &types.NetworkIdentifier{
			Blockchain: "?",
		},
	}
	invalidCurrency = &Configuration{
		Construction: &ConstructionConfiguration{
			Currency: &types.Currency{
				Decimals: 12,
			},
		},
	}
	invalidCurve = &Configuration{
		Construction: &ConstructionConfiguration{
			CurveType: "hello",
		},
	}
	invalidAccountingModel = &Configuration{
		Construction: &ConstructionConfiguration{
			AccountingModel: "hello",
		},
	}
	invalidMinimumBalance = &Configuration{
		Construction: &ConstructionConfiguration{
			MinimumBalance: "-1000",
		},
	}
	invalidMaximumFee = &Configuration{
		Construction: &ConstructionConfiguration{
			MaximumFee: "hello",
		},
	}
)

func TestLoadConfiguration(t *testing.T) {
	var tests = map[string]struct {
		provided *Configuration
		expected *Configuration

		err bool
	}{
		"nothing provided": {
			provided: &Configuration{},
			expected: DefaultConfiguration(),
		},
		"no overwrite": {
			provided: whackyConfig,
			expected: whackyConfig,
		},
		"overwrite missing": {
			provided: &Configuration{
				Construction: &ConstructionConfiguration{},
				Data:         &DataConfiguration{},
			},
			expected: DefaultConfiguration(),
		},
		"invalid network": {
			provided: invalidNetwork,
			err:      true,
		},
		"invalid currency": {
			provided: invalidCurrency,
			err:      true,
		},
		"invalid curve type": {
			provided: invalidCurve,
			err:      true,
		},
		"invalid accounting model": {
			provided: invalidAccountingModel,
			err:      true,
		},
		"invalid minimum balance": {
			provided: invalidMinimumBalance,
			err:      true,
		},
		"invalid maximum fee": {
			provided: invalidMaximumFee,
			err:      true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Write configuration file to tempdir
			tmpfile, err := ioutil.TempFile("", "test.json")
			assert.NoError(t, err)
			defer os.Remove(tmpfile.Name())

			err = utils.SerializeAndWrite(tmpfile.Name(), test.provided)
			assert.NoError(t, err)

			// Check if expected fields populated
			config, err := LoadConfiguration(tmpfile.Name())
			if test.err {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, config)
			}
			assert.NoError(t, tmpfile.Close())
		})
	}
}
