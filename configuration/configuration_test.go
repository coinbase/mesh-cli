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
	"io/ioutil"
	"os"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/stretchr/testify/assert"
)

var (
	startIndex        = int64(89)
	badStartIndex     = int64(-10)
	goodCoverage      = float64(0.33)
	badCoverage       = float64(-2)
	endTip            = false
	historicalEnabled = true
	fakeWorkflows     = []*job.Workflow{
		{
			Name:        string(job.CreateAccount),
			Concurrency: job.ReservedWorkflowConcurrency,
			Scenarios: []*job.Scenario{
				{
					Name:    "blah",
					Actions: []*job.Action{},
				},
			},
		},
		{
			Name:        string(job.RequestFunds),
			Concurrency: job.ReservedWorkflowConcurrency,
			Scenarios: []*job.Scenario{
				{
					Name:    "blah",
					Actions: []*job.Action{},
				},
			},
		},
	}
	whackyConfig = &Configuration{
		Network: &types.NetworkIdentifier{
			Blockchain: "sweet",
			Network:    "sweeter",
		},
		OnlineURL:            "http://hasudhasjkdk",
		MaxOnlineConnections: 10,
		HTTPTimeout:          21,
		MaxRetries:           1000,
		MaxSyncConcurrency:   12,
		TipDelay:             1231,
		Construction: &ConstructionConfiguration{
			OfflineURL:            "https://ashdjaksdkjshdk",
			MaxOfflineConnections: 21,
			StaleDepth:            12,
			BroadcastLimit:        200,
			BlockBroadcastLimit:   992,
			StatusPort:            21,
			Workflows: append(
				fakeWorkflows,
				&job.Workflow{
					Name:        "transfer",
					Concurrency: 100,
				},
			),
		},
		Data: &DataConfiguration{
			ActiveReconciliationConcurrency:   100,
			InactiveReconciliationConcurrency: 2938,
			InactiveReconciliationFrequency:   3,
			ReconciliationDisabled:            false,
			HistoricalBalanceEnabled:          &historicalEnabled,
			StartIndex:                        &startIndex,
			StatusPort:                        123,
			EndConditions: &DataEndConditions{
				ReconciliationCoverage: &goodCoverage,
			},
		},
	}
	invalidNetwork = &Configuration{
		Network: &types.NetworkIdentifier{
			Blockchain: "?",
		},
	}
	invalidPrefundedAccounts = &Configuration{
		Construction: &ConstructionConfiguration{
			PrefundedAccounts: []*storage.PrefundedAccount{
				{
					PrivateKeyHex: "hello",
				},
			},
		},
	}
	invalidStartIndex = &Configuration{
		Data: &DataConfiguration{
			StartIndex: &badStartIndex,
		},
	}
	multipleEndConditions = &Configuration{
		Data: &DataConfiguration{
			EndConditions: &DataEndConditions{
				Index: &startIndex,
				Tip:   &endTip,
			},
		},
	}
	invalidEndIndex = &Configuration{
		Data: &DataConfiguration{
			EndConditions: &DataEndConditions{
				Index: &badStartIndex,
			},
		},
	}
	invalidReconciliationCoverage = &Configuration{
		Data: &DataConfiguration{
			EndConditions: &DataEndConditions{
				ReconciliationCoverage: &badCoverage,
			},
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
				Construction: &ConstructionConfiguration{
					Workflows: fakeWorkflows,
				},
				Data: &DataConfiguration{},
			},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.Construction = &ConstructionConfiguration{
					OfflineURL:            DefaultURL,
					MaxOfflineConnections: DefaultMaxOfflineConnections,
					StaleDepth:            DefaultStaleDepth,
					BroadcastLimit:        DefaultBroadcastLimit,
					BlockBroadcastLimit:   DefaultBlockBroadcastLimit,
					StatusPort:            DefaultStatusPort,
					Workflows:             fakeWorkflows,
				}

				return cfg
			}(),
		},
		"overwrite missing with DSL": {
			provided: &Configuration{
				Construction: &ConstructionConfiguration{
					ConstructorDSLFile: "testdata/test.ros",
				},
				Data: &DataConfiguration{},
			},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.Construction = &ConstructionConfiguration{
					OfflineURL:            DefaultURL,
					MaxOfflineConnections: DefaultMaxOfflineConnections,
					StaleDepth:            DefaultStaleDepth,
					BroadcastLimit:        DefaultBroadcastLimit,
					BlockBroadcastLimit:   DefaultBlockBroadcastLimit,
					StatusPort:            DefaultStatusPort,
					Workflows:             fakeWorkflows,
					ConstructorDSLFile:    "testdata/test.ros",
				}

				return cfg
			}(),
		},
		"invalid network": {
			provided: invalidNetwork,
			err:      true,
		},
		"invalid prefunded accounts": {
			provided: invalidPrefundedAccounts,
			err:      true,
		},
		"invalid start index": {
			provided: invalidStartIndex,
			err:      true,
		},
		"invalid end index": {
			provided: invalidEndIndex,
			err:      true,
		},
		"invalid reconciliation coverage": {
			provided: invalidReconciliationCoverage,
			err:      true,
		},
		"invalid reconciliation coverage (reconciliation disabled)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					ReconciliationDisabled: true,
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &goodCoverage,
					},
				},
			},
			err: true,
		},
		"invalid reconciliation coverage (balance tracking disabled)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					BalanceTrackingDisabled: true,
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &goodCoverage,
					},
				},
			},
			err: true,
		},
		"invalid reconciliation coverage (ignore reconciliation error)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					IgnoreReconciliationError: true,
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &goodCoverage,
					},
				},
			},
			err: true,
		},
		"empty workflows": {
			provided: &Configuration{
				Construction: &ConstructionConfiguration{
					Workflows: []*job.Workflow{},
				},
			},
			err: true,
		},
		"non-existent dsl file": {
			provided: &Configuration{
				Construction: &ConstructionConfiguration{
					ConstructorDSLFile: "test.ros",
				},
			},
			err: true,
		},
		"multiple end conditions": {
			provided: multipleEndConditions,
			expected: func() *Configuration {
				def := DefaultConfiguration()
				def.Data.EndConditions = multipleEndConditions.Data.EndConditions

				return def
			}(),
			err: false,
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
			config, err := LoadConfiguration(context.Background(), tmpfile.Name())
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
