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

import "github.com/coinbase/rosetta-sdk-go/types"

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/asserter"
)

var (
	basicNetwork = &types.NetworkIdentifier{
		Blockchain: "blockchain",
		Network:    "network",
	}

	basicBlock = &types.BlockIdentifier{
		Index: 10,
		Hash:  "block 10",
	}

	allowedOperationTypes = []string{"OUTPUT", "INPUT", "TRANSFER"}

	allowedOperationStatuses = []*types.OperationStatus{
		{
			Status:     "SUCCESS",
			Successful: true,
		},
		{
			Status:     "SKIPPED",
			Successful: true,
		},
	}

	allowedErrors = []*types.Error{
		{
			Code:      4,
			Message:   "Block not found",
			Retriable: false,
		},
		{
			Code:      0,
			Message:   "Endpoint not implemented",
			Retriable: false,
		},
		{
			Code:      3,
			Message:   "Bitcoind error",
			Retriable: false,
		},
	}

	timestampStartIndex = int64(6)
)

func TestSortArrayFields(t *testing.T) {
	var clientConfiguration = &asserter.Configuration{
		NetworkIdentifier:          basicNetwork,
		GenesisBlockIdentifier:     basicBlock,
		AllowedOperationTypes:      allowedOperationTypes,
		AllowedOperationStatuses:   allowedOperationStatuses,
		AllowedErrors:              allowedErrors,
		AllowedTimestampStartIndex: timestampStartIndex,
	}
	var assert = assert.New(t)
	sortArrayFieldsOnConfiguration(clientConfiguration)
	assert.Equal([]string{"INPUT", "OUTPUT", "TRANSFER"}, clientConfiguration.AllowedOperationTypes)
	assert.Equal([]*types.OperationStatus{
		{
			Status:     "SKIPPED",
			Successful: true,
		},
		{
			Status:     "SUCCESS",
			Successful: true,
		},
	}, clientConfiguration.AllowedOperationStatuses)
	assert.Equal([]*types.Error{
		{
			Code:      0,
			Message:   "Endpoint not implemented",
			Retriable: false,
		},
		{
			Code:      3,
			Message:   "Bitcoind error",
			Retriable: false,
		},
		{
			Code:      4,
			Message:   "Block not found",
			Retriable: false,
		},
	}, clientConfiguration.AllowedErrors)
}
