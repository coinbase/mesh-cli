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
