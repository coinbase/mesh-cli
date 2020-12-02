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

package processor

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	opAmountCurrency = &types.AccountCurrency{
		Account: &types.AccountIdentifier{
			Address: "hello",
		},
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}
)

func TestExemptFuncExemptAccounts(t *testing.T) {
	var tests = map[string]struct {
		exemptAccounts []*types.AccountCurrency
		exempt         bool
	}{
		"no exempt accounts": {},
		"account not exempt": {
			exemptAccounts: []*types.AccountCurrency{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: opAmountCurrency.Currency,
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: opAmountCurrency.Currency,
				},
			},
		},
		"account is exempt": {
			exemptAccounts: []*types.AccountCurrency{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: opAmountCurrency.Currency,
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: opAmountCurrency.Currency,
				},
				opAmountCurrency,
			},
			exempt: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			helper := NewBalanceStorageHelper(
				nil,
				nil,
				nil,
				false,
				test.exemptAccounts,
				false,
				nil,
				false,
			)

			result := helper.ExemptFunc()(&types.Operation{
				Account: opAmountCurrency.Account,
				Amount: &types.Amount{
					Value:    "100",
					Currency: opAmountCurrency.Currency,
				},
			})

			assert.Equal(t, test.exempt, result)
		})
	}
}

func TestExemptFuncInterestingParsing(t *testing.T) {
	var tests = map[string]struct {
		interestingAddresses []string
		exempt               bool
	}{
		"no interesting accounts": {
			exempt: true,
		},
		"account interesting": {
			interestingAddresses: []string{opAmountCurrency.Account.Address, "addr 3"},
			exempt:               false,
		},
		"account not interesting": {
			interestingAddresses: []string{"addr2"},
			exempt:               true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			helper := NewBalanceStorageHelper(
				nil,
				nil,
				nil,
				false,
				nil,
				true,
				nil,
				false,
			)

			for _, addr := range test.interestingAddresses {
				helper.AddInterestingAddress(addr)
			}

			result := helper.ExemptFunc()(&types.Operation{
				Account: opAmountCurrency.Account,
				Amount: &types.Amount{
					Value:    "100",
					Currency: opAmountCurrency.Currency,
				},
			})

			assert.Equal(t, test.exempt, result)
		})
	}
}
