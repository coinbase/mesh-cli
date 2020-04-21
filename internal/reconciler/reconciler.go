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

package reconciler

import (
	"context"
	"fmt"
	"reflect"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

type Reconciler interface {
	QueueChanges(
		ctx context.Context,
		changes []*storage.BalanceChange,
	) error

	Reconcile(ctx context.Context) error
}

// ExtractAmount returns the types.Amount from a slice of types.Balance
// pertaining to an AccountAndCurrency.
func ExtractAmount(
	balances []*types.Amount,
	currency *types.Currency,
) (*types.Amount, error) {
	for _, b := range balances {
		if !reflect.DeepEqual(b.Currency, currency) {
			continue
		}

		return b, nil
	}

	return nil, fmt.Errorf("could not extract amount for %+v", currency)
}

type AccountCurrency struct {
	Account  *types.AccountIdentifier
	Currency *types.Currency
}

// ContainsAccountCurrency returns a boolean indicating if a
// AccountCurrency slice already contains an Account and Currency combination.
func ContainsAccountCurrency(
	arr []*AccountCurrency,
	change *AccountCurrency,
) bool {
	for _, a := range arr {
		if reflect.DeepEqual(a.Account, change.Account) &&
			reflect.DeepEqual(a.Currency, change.Currency) {
			return true
		}
	}

	return false
}

func GetCurrencyBalance(
	ctx context.Context,
	fetcher *fetcher.Fetcher,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, string, error) {
	liveBlock, liveBalances, _, err := fetcher.AccountBalanceRetry(
		ctx,
		network,
		account,
		block,
	)
	if err != nil {
		return nil, "", err
	}

	liveAmount, err := ExtractAmount(liveBalances, currency)
	if err != nil {
		return nil, "", err
	}

	return liveBlock, liveAmount.Value, nil
}
