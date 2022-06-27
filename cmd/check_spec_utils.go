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
	"github.com/fatih/color"
)

var (
	errErrorEmptyMessage           = errors.New("Error object can't have empty message")
	errErrorNegativeCode           = errors.New("Error object can't have negative code")
	errAccountNullPointer          = errors.New("Null pointer to Account object")
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
	accountCoins   checkSpecAPI = "/account/coins"
	block          checkSpecAPI = "/block"
	errorObject    checkSpecAPI = "error object"
	modes          checkSpecAPI = "modes"

	networkIDs      checkSpecRequirement = "network_identifiers is required"
	offlineMode     checkSpecRequirement = "endpoint should work in offline mode"
	staticNetworkID checkSpecRequirement = "network_identifier must be static"
	version         checkSpecRequirement = "field version is required"
	allow           checkSpecRequirement = "field allow is required"

	blockID    checkSpecRequirement = "block_identifier is required"
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
		setValidationStatusFailed(output, diffURLs)
	}

	return output
}

func markAllValidationsFailed(output checkSpecOutput) {
	for k := range output.validation {
		output.validation[k] = checkSpecFailure
	}
}

func setValidationStatusFailed(output checkSpecOutput, req checkSpecRequirement) {
	output.validation[req] = checkSpecFailure
}

func validateErrorObject(err *fetcher.Error, output checkSpecOutput) {
	if err != nil {
		if err.ClientErr != nil && isNegative(int64(err.ClientErr.Code)) {
			printError("%v\n", errErrorNegativeCode)
			setValidationStatusFailed(output, errorCode)
		}

		if err.ClientErr != nil && isEmpty(err.ClientErr.Message) {
			printError("%v\n", errErrorEmptyMessage)
			setValidationStatusFailed(output, errorMessage)
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
	printInfo("%v\n", "|           API            |                              Requirement                          |   Status  |")
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
