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
	"os/exec"
	"path"
	"runtime"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"github.com/stretchr/testify/assert"
)

var (
	startIndex         = int64(89)
	badStartIndex      = int64(-10)
	goodCoverage       = float64(0.33)
	badCoverage        = float64(-2)
	endTip             = false
	historicalDisabled = false
	fakeWorkflows      = []*job.Workflow{
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
		OnlineURL:               "http://hasudhasjkdk",
		MaxOnlineConnections:    10,
		HTTPTimeout:             21,
		MaxRetries:              1000,
		MaxSyncConcurrency:      12,
		TipDelay:                1231,
		MaxReorgDepth:           12,
		SeenBlockWorkers:        300,
		SerialBlockWorkers:      200,
		ErrorStackTraceDisabled: false,
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
			HistoricalBalanceDisabled:         &historicalDisabled,
			StartIndex:                        &startIndex,
			StatusPort:                        123,
			EndConditions: &DataEndConditions{
				ReconciliationCoverage: &ReconciliationCoverage{
					Coverage: goodCoverage,
				},
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
			PrefundedAccounts: []*modules.PrefundedAccount{
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
				ReconciliationCoverage: &ReconciliationCoverage{
					Coverage: badCoverage,
				},
			},
		},
	}
)

func TestLoadConfiguration(t *testing.T) {
	var (
		goodAccountCount = int64(10)
		badAccountCount  = int64(-10)
	)
	var tests = map[string]struct {
		provided *Configuration
		expected *Configuration

		err bool
	}{
		"nothing provided": {
			provided: &Configuration{},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.SeenBlockWorkers = runtime.NumCPU()
				cfg.SerialBlockWorkers = runtime.NumCPU()

				return cfg
			}(),
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
				cfg.SeenBlockWorkers = runtime.NumCPU()
				cfg.SerialBlockWorkers = runtime.NumCPU()
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
					ConstructorDSLFile: "test.ros",
				},
				Data: &DataConfiguration{},
			},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.SeenBlockWorkers = runtime.NumCPU()
				cfg.SerialBlockWorkers = runtime.NumCPU()
				cfg.Construction = &ConstructionConfiguration{
					OfflineURL:            DefaultURL,
					MaxOfflineConnections: DefaultMaxOfflineConnections,
					StaleDepth:            DefaultStaleDepth,
					BroadcastLimit:        DefaultBroadcastLimit,
					BlockBroadcastLimit:   DefaultBlockBroadcastLimit,
					StatusPort:            DefaultStatusPort,
					Workflows:             fakeWorkflows,
					ConstructorDSLFile:    "test.ros",
				}

				return cfg
			}(),
		},
		"transfer workflow": {
			provided: &Configuration{
				Construction: &ConstructionConfiguration{
					Workflows: []*job.Workflow{
						{
							Name:        "transfer",
							Concurrency: 10,
						},
					},
				},
				Data: &DataConfiguration{},
			},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.SeenBlockWorkers = runtime.NumCPU()
				cfg.SerialBlockWorkers = runtime.NumCPU()
				cfg.Construction = &ConstructionConfiguration{
					OfflineURL:            DefaultURL,
					MaxOfflineConnections: DefaultMaxOfflineConnections,
					StaleDepth:            DefaultStaleDepth,
					BroadcastLimit:        DefaultBroadcastLimit,
					BlockBroadcastLimit:   DefaultBlockBroadcastLimit,
					StatusPort:            DefaultStatusPort,
					Workflows: []*job.Workflow{
						{
							Name:        "transfer",
							Concurrency: 10,
						},
					},
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
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage: goodCoverage,
						},
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
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage: goodCoverage,
						},
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
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage: goodCoverage,
						},
					},
				},
			},
			err: true,
		},
		"valid reconciliation coverage (with account count)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage:     goodCoverage,
							AccountCount: &goodAccountCount,
							Index:        &goodAccountCount,
						},
					},
				},
			},
			expected: func() *Configuration {
				cfg := DefaultConfiguration()
				cfg.SeenBlockWorkers = runtime.NumCPU()
				cfg.SerialBlockWorkers = runtime.NumCPU()
				cfg.Data.EndConditions = &DataEndConditions{
					ReconciliationCoverage: &ReconciliationCoverage{
						Coverage:     goodCoverage,
						AccountCount: &goodAccountCount,
						Index:        &goodAccountCount,
					},
				}

				return cfg
			}(),
		},
		"invalid reconciliation coverage (with account count)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage:     goodCoverage,
							AccountCount: &badAccountCount,
						},
					},
				},
			},
			err: true,
		},
		"invalid reconciliation coverage (with index)": {
			provided: &Configuration{
				Data: &DataConfiguration{
					EndConditions: &DataEndConditions{
						ReconciliationCoverage: &ReconciliationCoverage{
							Coverage: goodCoverage,
							Index:    &badAccountCount,
						},
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
					ConstructorDSLFile: "blah.ros",
				},
			},
			err: true,
		},
		"multiple end conditions": {
			provided: multipleEndConditions,
			expected: func() *Configuration {
				def := DefaultConfiguration()
				def.SeenBlockWorkers = runtime.NumCPU()
				def.SerialBlockWorkers = runtime.NumCPU()
				def.Data.EndConditions = multipleEndConditions.Data.EndConditions

				return def
			}(),
			err: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dir, err := utils.CreateTempDir()
			assert.NoError(t, err)
			defer utils.RemoveTempDir(dir)

			filePath := path.Join(dir, "test.json")
			err = utils.SerializeAndWrite(filePath, test.provided)
			assert.NoError(t, err)

			// Copy test.ros to temp dir
			cmd := exec.Command("cp", "testdata/test.ros", path.Join(dir, "test.ros"))
			assert.NoError(t, cmd.Run())

			// Check if expected fields populated
			config, err := LoadConfiguration(context.Background(), filePath)
			if test.err {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)

				// Ensure test.ros expected file path is right
				if test.expected.Construction != nil && len(test.expected.Construction.ConstructorDSLFile) > 0 {
					test.expected.Construction.ConstructorDSLFile = path.Join(dir, test.expected.Construction.ConstructorDSLFile)
				}
				assert.Equal(t, test.expected, config)
			}
		})
	}
}
