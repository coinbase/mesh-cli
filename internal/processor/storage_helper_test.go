package processor

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	opAmountCurrency = &reconciler.AccountCurrency{
		Account: &types.AccountIdentifier{
			Address: "hello",
		},
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}
)

func TestExemptFunc(t *testing.T) {
	var tests = map[string]struct {
		exemptAccounts []*reconciler.AccountCurrency
		exempt         bool
	}{
		"no exempt accounts": {},
		"account not exempt": {
			exemptAccounts: []*reconciler.AccountCurrency{
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
			exemptAccounts: []*reconciler.AccountCurrency{
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
			helper := NewBlockStorageHelper(nil, nil, false, test.exemptAccounts)

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
