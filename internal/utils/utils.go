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
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// CreateTempDir creates a directory in
// /tmp for usage within testing.
func CreateTempDir() (string, error) {
	storageDir, err := ioutil.TempDir("", "rosetta-cli")
	if err != nil {
		return "", err
	}

	return storageDir, nil
}

// RemoveTempDir deletes a directory at
// a provided path for usage within testing.
func RemoveTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// AddStringValues adds string amounts using
// big.Int.
func AddStringValues(
	a string,
	b string,
) (string, error) {
	aVal, ok := new(big.Int).SetString(a, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", a)
	}

	bVal, ok := new(big.Int).SetString(b, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", b)
	}

	newVal := new(big.Int).Add(aVal, bVal)
	return newVal.String(), nil
}

// SubtractStringValues subtracts a-b using
// big.Int.
func SubtractStringValues(
	a string,
	b string,
) (string, error) {
	aVal, ok := new(big.Int).SetString(a, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", a)
	}

	bVal, ok := new(big.Int).SetString(b, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", b)
	}

	newVal := new(big.Int).Sub(aVal, bVal)
	return newVal.String(), nil
}

// AccountString returns a byte slice representing a *types.AccountIdentifier.
// This byte slice automatically handles the existence of *types.SubAccount
// detail.
func AccountString(account *types.AccountIdentifier) string {
	if account.SubAccount == nil {
		return account.Address
	}

	if account.SubAccount.Metadata == nil {
		return fmt.Sprintf(
			"%s:%s",
			account.Address,
			account.SubAccount.Address,
		)
	}

	// TODO: handle SubAccount.Metadata
	// that contains pointer values.
	return fmt.Sprintf(
		"%s:%s:%v",
		account.Address,
		account.SubAccount.Address,
		account.SubAccount.Metadata,
	)
}

// CurrencyString is used to identify a *types.Currency
// in an account's map of currencies. It is not feasible
// to create a map of [types.Currency]*types.Amount
// because types.Currency contains a metadata pointer
// that would prevent any equality.
func CurrencyString(currency *types.Currency) string {
	if currency.Metadata == nil {
		return fmt.Sprintf("%s:%d", currency.Symbol, currency.Decimals)
	}

	// TODO: Handle currency.Metadata
	// that has pointer value.
	return fmt.Sprintf(
		"%s:%d:%v",
		currency.Symbol,
		currency.Decimals,
		currency.Metadata,
	)
}
