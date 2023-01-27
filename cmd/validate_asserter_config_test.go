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
	"github.com/stretchr/testify/assert"
	"testing"
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
	// Confirm nil defaults to 1
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.TimestampStartIndex = nil
	confirmError(t, networkAllow, asserterConfiguration)
	asserterConfiguration.AllowedTimestampStartIndex = 1
	confirmSuccess(t, networkAllow, asserterConfiguration)

	networkAllow, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedTimestampStartIndex = 567
	confirmError(t, networkAllow, asserterConfiguration)
}

func TestOperationTypes(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.OperationTypes[1] = "mismatchType"
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow.OperationTypes = append(generateOperationTypes(), "extra")
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, _ = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedOperationTypes = nil
	confirmError(t, networkAllow, asserterConfiguration)
}

func TestOperationStatuses(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.OperationStatuses[0].Successful = !networkAllow.OperationStatuses[0].Successful
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, _ = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedOperationStatuses[1].Status = "mismatchStatus"
	confirmError(t, networkAllow, asserterConfiguration)

	_, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedOperationStatuses = append(generateOperationStatuses(),
		&types.OperationStatus{Status: "extra"})
	confirmError(t, networkAllow, asserterConfiguration)

	_, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	networkAllow.OperationStatuses = nil
	confirmError(t, networkAllow, asserterConfiguration)
}

func TestErrors(t *testing.T) {
	networkAllow, asserterConfiguration := generateNetworkAllowAndAsserterConfiguration()
	networkAllow.Errors[0].Code = 123
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, _ = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedErrors[1].Message = "mismatchMessage"
	confirmError(t, networkAllow, asserterConfiguration)

	_, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	networkAllow.Errors[0].Details = map[string]interface{}{"key": "value"}
	asserterConfiguration.AllowedErrors[0].Details = map[string]interface{}{"key": "differentValue"}
	confirmError(t, networkAllow, asserterConfiguration)

	networkAllow, asserterConfiguration = generateNetworkAllowAndAsserterConfiguration()
	asserterConfiguration.AllowedErrors = append(asserterConfiguration.AllowedErrors,
		&types.Error{Code: 123, Message: "extra"})
	confirmError(t, networkAllow, asserterConfiguration)
}

// Generate simple configs for testing
// Generators used internally below are so they are logically equal but can be mutated separately
func generateNetworkAllowAndAsserterConfiguration() (
	*types.Allow, *asserter.Configuration,
) {
	var tsi int64 = 5
	allow := &types.Allow{
		OperationStatuses:   generateOperationStatuses(),
		OperationTypes:      generateOperationTypes(),
		Errors:              generateErrors(),
		TimestampStartIndex: &tsi,
	}
	config := &asserter.Configuration{
		AllowedOperationStatuses:   generateOperationStatuses(),
		AllowedOperationTypes:      generateOperationTypes(),
		AllowedErrors:              generateErrors(),
		AllowedTimestampStartIndex: tsi,
	}

	return allow, config
}

func generateOperationTypes() []string {
	return []string{"type0", "type1"}
}

func generateOperationStatuses() []*types.OperationStatus {
	return []*types.OperationStatus{
		{
			Successful: true,
			Status:     "status0",
		},
		{
			// Successful: false
			Status: "status1",
		},
	}
}

func generateErrors() []*types.Error {
	return []*types.Error{
		{
			Code:    1,
			Message: "message1",
		},
		{
			Code:    2,
			Message: "message2",
		},
	}
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
