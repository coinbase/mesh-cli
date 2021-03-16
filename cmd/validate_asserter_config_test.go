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
	assert.NoError(t, validateNetworkAndAsserterAllowMatch(networkAllow, asserterConfiguration))
}

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
	operationTypes := []string{"type0", "type1"}
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
