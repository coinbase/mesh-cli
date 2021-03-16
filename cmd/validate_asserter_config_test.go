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

package cmd

import (
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestMatch(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	confirmSuccess(t, networkAllow, asserterConfiguration)
}

func TestNil(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	confirmError(t, nil, asserterConfiguration)
	confirmError(t, networkAllow, nil)
}

func TestTsi(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.TimestampStartIndex = nil
	confirmSuccess(t, networkAllow, asserterConfiguration)

	networkAllow, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedTimestampStartIndex = 567
	confirmError(t, networkAllow, asserterConfiguration)
}

func TestOperationTypes(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.OperationTypes = generateOperationTypes()
	networkAllow.OperationTypes[1] = "mismatchType"
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, _ = generateNetworkAllowAndAsserterConfiguration()
	networkAllow.OperationTypes = append(networkAllow.OperationTypes, "extra")
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, _ = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedOperationTypes = nil
	confirmError(t, networkAllow, asserterConfiguration)
}

// Generate simple configs for testing
func generateNetworkAllowAndAsserterConfiguration() (
	*types.Allow, *asserter.Configuration,
) {
	operationStatuses := []*types.OperationStatus{
		{
			Successful: true,
			Status: "status0",
		},
		{
			// Successful: false
			Status: "status1",
		},
	}
	operationTypes := generateOperationTypes()
	errors := []*types.Error{
		{
			Code: 1,
			Message: "message1",
		},
		{
			Code: 2,
			Message: "message2",
		},
	}
	var tsi int64 = 5

	allow := &types.Allow{
		OperationStatuses:       operationStatuses,
		OperationTypes:          operationTypes,
		Errors:                  errors,
		TimestampStartIndex:     &tsi,
	}
	config := &asserter.Configuration{
		AllowedOperationStatuses:   operationStatuses,
		AllowedOperationTypes:      operationTypes,
		AllowedErrors:              errors,
		AllowedTimestampStartIndex: tsi,
	}

	return allow, config
}

func generateOperationTypes() []string {
	return []string{"type0", "type1"}
}

func confirmSuccess(
	t *testing.T, networkAllow *types.Allow, asserterConfiguration *asserter.Configuration,
) {
	assert.NoError(t, validateNetworkAndAsserterAllowMatch(networkAllow, asserterConfiguration))
}

func confirmError(
	t *testing.T, networkAllow *types.Allow, asserterConfiguration *asserter.Configuration,
) {
	assert.Error(t, validateNetworkAndAsserterAllowMatch(networkAllow, asserterConfiguration))
}