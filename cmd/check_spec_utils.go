// Copyright 2022 Coinbase, Inc.
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
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
)

var (
	errBlockIdentifierNullPointer   = errors.New("Null pointer to BlockIdentifier object")
	errBlockIdentifierEmptyHash     = errors.New("BlockIdentifier can't have empty hash")
	errBlockIdentifierNegativeIndex = errors.New("BlockIdentifier can't have negative index")

	errTimestampNegative  = errors.New("Timestamp can't be negative")
	errVersionEmpty       = errors.New("Version can't be empty")
	errVersionNullPointer = errors.New("Null pointer to Version object")

	errOperationStatusEmptyStatus = errors.New("OperationStatus can't have empty status value")
	errOperationStatusNullPointer = errors.New("Null pointer to OperationStatus object")
	errOperationTypeEmpty         = errors.New("OperationType can't be empty")

	errErrorEmpty        = errors.New("Error object can't be empty")
	errErrorEmptyMessage = errors.New("Error object can't have empty message")
	errErrorNegativeCode = errors.New("Error object can't have negative code")

	errCallMethodEmptyName   = errors.New("Call method name can't be empty")
	errBalanceExemptionEmpty = errors.New("Balance exemption can't be empty")
	errAllowNullPointer      = errors.New("Null pointer to Allow object")

	errBalanceEmptyValue        = errors.New("Amount can't be empty")
	errCurrencyEmptySymbol      = errors.New("Currency can't have empty symbol")
	errCurrencyNegativeDecimals = errors.New("Currency can't have negative decimals")
	errAccountNullPointer       = errors.New("Null pointer to Account object")

	errCoinIdentifierEmpty         = errors.New("Coin identifier can't be empty")
	errCoinIdentifierNullPointer   = errors.New("Null pointer to coin identifier object")
	errBlockNotIdempotent          = errors.New("Multiple calls with the same hash don't return the same block")
	errBlockTip                    = errors.New("Unspecified block_identifier doesn't give the tip block")
	errRosettaConfigNoConstruction = errors.New("No construction element in Rosetta config")
)

type checkSpecAPI string
type checkSpecRequirement string
type checkSpecStatus string

const (
	networkList    checkSpecAPI = "/network/list"
	networkOptions checkSpecAPI = "/network/options"
	networkStatus  checkSpecAPI = "/network/status"
	accountBalance checkSpecAPI = "/account/balance"
	accountCoins   checkSpecAPI = "/account/coins"
	block          checkSpecAPI = "/block"
	errorObject    checkSpecAPI = "error object"
	modes          checkSpecAPI = "modes"

	networkIDs      checkSpecRequirement = "network_identifiers is required"
	offlineMode     checkSpecRequirement = "endpoint should work in offline mode"
	staticNetworkID checkSpecRequirement = "network_identifier must be static"
	version         checkSpecRequirement = "field version is required"
	allow           checkSpecRequirement = "field allow is required"

	currentBlockID   checkSpecRequirement = "current_block_identifier is required"
	currentBlockTime checkSpecRequirement = "current_block_timestamp is required"
	genesisBlockID   checkSpecRequirement = "genesis_block_identifier is required"

	blockID    checkSpecRequirement = "block_identifier is required"
	balances   checkSpecRequirement = "field balances is required"
	coins      checkSpecRequirement = "field coins is required"
	idempotent checkSpecRequirement = "same hash should return the same block"
	defaultTip checkSpecRequirement = "tip should be returned if block_identifier is not specified"

	errorCode    checkSpecRequirement = "error code is required"
	errorMessage checkSpecRequirement = "error message is required"
	diffURLs     checkSpecRequirement = "offline_url should be different from offline_url and not empty"

	checkSpecSuccess checkSpecStatus = "Success"
	checkSpecFailure checkSpecStatus = "Failure"
)

type checkSpecOutput struct {
	api        checkSpecAPI
	validation map[checkSpecRequirement]checkSpecStatus
}

func validateBlockIdentifier(blockID *types.BlockIdentifier) error {
	if blockID == nil {
		return errBlockIdentifierNullPointer
	}

	if isEmpty(blockID.Hash) {
		return errBlockIdentifierEmptyHash
	}

	if isNegative(blockID.Index) {
		return errBlockIdentifierNegativeIndex
	}

	return nil
}

func validateTimestamp(time int64) error {
	if isNegative(time) {
		return errTimestampNegative
	}

	return nil
}

func validateVersion(version string) error {
	if isEmpty(version) {
		return errVersionEmpty
	}

	return nil
}

func validateOperationStatuses(oss []*types.OperationStatus) error {
	for _, os := range oss {
		if os == nil {
			return errOperationStatusNullPointer
		}

		if isEmpty(os.Status) {
			return errOperationStatusEmptyStatus
		}
	}

	return nil
}

func validateOperationTypes(ots []string) error {
	for _, ot := range ots {
		if isEmpty(ot) {
			return errOperationTypeEmpty
		}
	}

	return nil
}

func validateErrors(errors []*types.Error) error {
	for _, err := range errors {
		if err == nil {
			return errErrorEmpty
		}

		if isNegative(int64(err.Code)) {
			return errErrorNegativeCode
		}

		if isEmpty(err.Message) {
			return errErrorEmptyMessage
		}
	}

	return nil
}

func validateCallMethods(methods []string) error {
	for _, m := range methods {
		if isEmpty(m) {
			return errCallMethodEmptyName
		}
	}

	return nil
}

func validateBalanceExemptions(exs []*types.BalanceExemption) error {
	for _, e := range exs {
		if e == nil {
			return errBalanceExemptionEmpty
		}
	}

	return nil
}

func validateBalances(balances []*types.Amount) error {
	for _, b := range balances {
		if err := validateBalance(b); err != nil {
			return err
		}
	}

	return nil
}

func validateBalance(amt *types.Amount) error {
	if isEmpty(amt.Value) {
		return errBalanceEmptyValue
	}

	if err := validateCurrency(amt.Currency); err != nil {
		return err
	}

	return nil
}

func validateCurrency(currency *types.Currency) error {
	if isEmpty(currency.Symbol) {
		return errCurrencyEmptySymbol
	}

	if isNegative(int64(currency.Decimals)) {
		return errCurrencyNegativeDecimals
	}

	return nil
}

func validateCoins(coins []*types.Coin) error {
	for _, coin := range coins {
		if err := validateCoinIdentifier(coin.CoinIdentifier); err != nil {
			return err
		}
		if err := validateBalance(coin.Amount); err != nil {
			return err
		}
	}

	return nil
}

func validateCoinIdentifier(coinID *types.CoinIdentifier) error {
	if coinID == nil {
		return errCoinIdentifierNullPointer
	}

	if isEmpty(coinID.Identifier) {
		return errCoinIdentifierEmpty
	}

	return nil
}

func twoModes() checkSpecOutput {
	output := checkSpecOutput{
		api: modes,
		validation: map[checkSpecRequirement]checkSpecStatus{
			diffURLs: checkSpecSuccess,
		},
	}

	if isEmpty(Config.OnlineURL) ||
		isEmpty(Config.Construction.OfflineURL) ||
		isEqual(Config.OnlineURL, Config.Construction.OfflineURL) {
		setValidationStatus(output, diffURLs, checkSpecFailure)
	}

	return output
}

func markAllValidationsFailed(output checkSpecOutput) {
	for k := range output.validation {
		output.validation[k] = checkSpecFailure
	}
}

func setValidationStatus(output checkSpecOutput, req checkSpecRequirement, status checkSpecStatus) {
	output.validation[req] = status
}

func validateErrorObject(err *fetcher.Error, output checkSpecOutput) {
	if err != nil {
		if err.ClientErr != nil && isNegative(int64(err.ClientErr.Code)) {
			printError("%v\n", errErrorNegativeCode)
			setValidationStatus(output, errorCode, checkSpecFailure)
		}

		if err.ClientErr != nil && isEmpty(err.ClientErr.Message) {
			printError("%v\n", errErrorEmptyMessage)
			setValidationStatus(output, errorMessage, checkSpecFailure)
		}
	}
}

func printInfo(format string, a ...interface{}) {
	fmt.Printf(format, a...)
}

func printError(format string, a ...interface{}) {
	fmt.Print(color.RedString(format, a...))
}

func printSuccess(format string, a ...interface{}) {
	fmt.Print(color.GreenString(format, a...))
}

func printValidationResult(format string, status checkSpecStatus, a ...interface{}) {
	if status == checkSpecFailure {
		printError(format, a...)
	} else {
		printSuccess(format, a...)
	}
}

func printCheckSpecOutputHeader() {
	printInfo("%v\n", "+--------------------------+-------------------------------------------------------------------+-----------+")
	printInfo("%v\n", "|           API            |                            Requirement                            |   Status  |")
	printInfo("%v\n", "+--------------------------+-------------------------------------------------------------------+-----------+")
}

func printCheckSpecOutputBody(output checkSpecOutput) {
	for k, v := range output.validation {
		// print api
		printInfo("%v", "|  ")
		printValidationResult("%v", v, output.api)
		for j := 0; j < 24-len(output.api); j++ {
			printInfo("%v", " ")
		}

		// print requirement description
		printInfo("%v", "|  ")
		printValidationResult("%v", v, k)
		for j := 0; j < 65-len(k); j++ {
			printInfo(" ")
		}

		// print validation status
		printInfo("%v", "|  ")
		printValidationResult("%v", v, v)
		for j := 0; j < 9-len(v); j++ {
			printInfo("%v", " ")
		}

		printInfo("%v\n", "|")
		printInfo("%v\n", "+--------------------------+-------------------------------------------------------------------+-----------+")
	}
}
