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

package utils

import (
	"math/big"
	"os"
	"path"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestCreateAndRemoveTempDir(t *testing.T) {
	dir, err := CreateTempDir()
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	assert.NoError(t, err)

	customPath := path.Join(dir, "test", "test2")
	_, err = os.Stat(customPath)
	assert.True(t, os.IsNotExist(err))

	assert.NoError(t, EnsurePathExists(customPath))
	_, err = os.Stat(path.Join(dir, "test"))
	assert.NoError(t, err)

	_, err = os.Stat(customPath)
	assert.NoError(t, err)

	// Write to path
	curr := &types.Currency{
		Symbol:   "BTC",
		Decimals: 8,
	}

	currPath := path.Join(customPath, "curr.json")
	err = SerializeAndWrite(currPath, curr)
	assert.NoError(t, err)

	_, err = os.Stat(currPath)
	assert.NoError(t, err)

	// Check write equal to read
	var newCurr types.Currency
	err = LoadAndParse(currPath, &newCurr)
	assert.NoError(t, err)
	assert.Equal(t, curr, &newCurr)

	// Test that we error when unknown fields
	var newBlock types.Block
	err = LoadAndParse(currPath, &newBlock)
	assert.Error(t, err)
	assert.Equal(t, types.Block{}, newBlock)

	RemoveTempDir(dir)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestCreateCommandPath(t *testing.T) {
	dir, err := CreateTempDir()
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	assert.NoError(t, err)

	net := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Mainnet",
	}

	dp, err := CreateCommandPath(dir, "test", net)
	assert.NoError(t, err)

	customPath := path.Join(dir, "test", types.Hash(net))
	assert.Equal(t, customPath, dp)
	_, err = os.Stat(customPath)
	assert.NoError(t, err)

	RemoveTempDir(dir)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestContainsString(t *testing.T) {
	var tests = map[string]struct {
		arr []string
		s   string

		contains bool
	}{
		"empty arr": {
			s: "hello",
		},
		"single arr": {
			arr:      []string{"hello"},
			s:        "hello",
			contains: true,
		},
		"single arr no elem": {
			arr: []string{"hello"},
			s:   "test",
		},
		"multiple arr with elem": {
			arr:      []string{"hello", "test"},
			s:        "test",
			contains: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.contains, ContainsString(test.arr, test.s))
		})
	}
}

func TestBigPow10(t *testing.T) {
	e := int32(12)
	v := big.NewFloat(10)

	for i := int32(0); i < e-1; i++ {
		v = new(big.Float).Mul(v, big.NewFloat(10))
	}

	assert.Equal(t, 0, new(big.Float).Sub(v, BigPow10(e)).Sign())
}

func TestPrettyAmount(t *testing.T) {
	var tests = map[string]struct {
		amount   *big.Int
		currency *types.Currency

		result string
	}{
		"no decimals": {
			amount:   big.NewInt(100),
			currency: &types.Currency{Symbol: "blah", Decimals: 0},
			result:   "100 blah",
		},
		"10 decimal": {
			amount:   big.NewInt(100),
			currency: &types.Currency{Symbol: "other", Decimals: 10},
			result:   "0.0000000100 other",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.result, PrettyAmount(test.amount, test.currency))
		})
	}
}

func TestMilliseconds(t *testing.T) {
	assert.True(t, Milliseconds() > asserter.MinUnixEpoch)
	assert.True(t, Milliseconds() < asserter.MaxUnixEpoch)
}

func TestRandomNumber(t *testing.T) {
	minAmount := big.NewInt(10)
	maxAmount := big.NewInt(13)

	// somewhat crude but its fast (should be infinitely small chance we don't get all possible
	// values in small range)
	for i := 0; i < 10000; i++ {
		result := RandomNumber(minAmount, maxAmount)
		assert.NotEqual(t, -1, new(big.Int).Sub(result, minAmount).Sign())
		assert.Equal(t, 1, new(big.Int).Sub(maxAmount, result).Sign())
	}
}
