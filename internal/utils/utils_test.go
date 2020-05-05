package utils

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestGetAccountString(t *testing.T) {
	var tests = map[string]struct {
		account *types.AccountIdentifier
		key     string
	}{
		"simple account": {
			account: &types.AccountIdentifier{
				Address: "hello",
			},
			key: "hello",
		},
		"subaccount": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
				},
			},
			key: "hello:stake",
		},
		"subaccount with string metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": "neat",
					},
				},
			},
			key: "hello:stake:map[cool:neat]",
		},
		"subaccount with number metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": 1,
					},
				},
			},
			key: "hello:stake:map[cool:1]",
		},
		"subaccount with complex metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool":    1,
						"awesome": "neat",
					},
				},
			},
			key: "hello:stake:map[awesome:neat cool:1]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.key, AccountString(test.account))
		})
	}
}

func TestCurrencyString(t *testing.T) {
	var tests = map[string]struct {
		currency *types.Currency
		key      string
	}{
		"simple currency": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			key: "BTC:8",
		},
		"currency with string metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
				},
			},
			key: "BTC:8:map[issuer:satoshi]",
		},
		"currency with number metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": 1,
				},
			},
			key: "BTC:8:map[issuer:1]",
		},
		"currency with complex metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
					"count":  10,
				},
			},
			key: "BTC:8:map[count:10 issuer:satoshi]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.key, CurrencyString(test.currency))
		})
	}
}
