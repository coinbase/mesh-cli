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

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	cliErrs "github.com/coinbase/rosetta-cli/pkg/errors"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// Common helper across Construction and Data
// Issues an RPC to fetch /network/options, and extracts the `Allow`
// Reads the JSON file at `asserterConfigurationFile` and loads into a Go object
// Validates the `Allow`s across both objects match
func validateNetworkOptionsMatchesAsserterConfiguration(
	ctx context.Context, f *fetcher.Fetcher, network *types.NetworkIdentifier,
	asserterConfigurationFile string,
) error {
	var asserterConfiguration asserter.Configuration
	if err := utils.LoadAndParse(asserterConfigurationFile, &asserterConfiguration); err != nil {
		return fmt.Errorf("failed to load and parse asserter configuration file %s: %w", asserterConfigurationFile, err)
	}

	resp, fetchErr := f.NetworkOptions(ctx, network, nil)
	if fetchErr != nil {
		return fmt.Errorf("failed to get network options: %w", fetchErr.Err)
	}

	err := validateNetworkAndAsserterAllowMatch(resp.Allow, &asserterConfiguration)
	if err != nil {
		return fmt.Errorf("failed to validate network options allowlist with asserter configuration: %w", err)
	}

	return nil
}

func validateNetworkAndAsserterAllowMatch(
	networkAllow *types.Allow, asserterConfiguration *asserter.Configuration,
) error {
	if networkAllow == nil {
		return cliErrs.ErrNetworkOptionsAllowlistIsNil
	}
	if asserterConfiguration == nil {
		return cliErrs.ErrAsserterConfigurationIsNil
	}

	if err := verifyTimestampStartIndex(
		networkAllow.TimestampStartIndex, asserterConfiguration.AllowedTimestampStartIndex,
	); err != nil {
		return fmt.Errorf("failed to verify timestamp start index: %w", err)
	}

	if err := verifyOperationTypes(
		networkAllow.OperationTypes, asserterConfiguration.AllowedOperationTypes,
	); err != nil {
		return fmt.Errorf("failed to verify operation types: %w", err)
	}

	if err := verifyOperationStatuses(
		networkAllow.OperationStatuses, asserterConfiguration.AllowedOperationStatuses,
	); err != nil {
		return fmt.Errorf("failed to verify operation statuses: %w", err)
	}

	if err := verifyErrors(
		networkAllow.Errors, asserterConfiguration.AllowedErrors,
	); err != nil {
		return fmt.Errorf("failed to verify errors: %w", err)
	}

	return nil
}

func verifyTimestampStartIndex(networkTsi *int64, assertTsi int64) error {
	var networkTsiVal int64 = 1
	if networkTsi != nil { // This field is optional and defaults to all allowed
		networkTsiVal = *networkTsi
	}
	if networkTsiVal != assertTsi {
		return fmt.Errorf("network options timestamp start index %d, asserter configuration timestamp start index %d: %w", networkTsiVal, assertTsi, cliErrs.ErrTimestampStartIndexMismatch)
	}

	return nil
}

func verifyOperationTypes(networkOt, asserterOt []string) error {
	if len(networkOt) != len(asserterOt) {
		return fmt.Errorf("network options operation type length %d, asserter configuration operation type length %d: %w", len(networkOt), len(asserterOt), cliErrs.ErrOperationTypeLengthMismatch)
	}

	sort.Strings(networkOt)
	sort.Strings(asserterOt)

	for i, networkOperationType := range networkOt {
		asserterOperationType := asserterOt[i]
		if networkOperationType != asserterOperationType {
			return fmt.Errorf("network options operation type %s, asserter configuration operation type %s: %w", networkOperationType, asserterOperationType, cliErrs.ErrOperationTypeMismatch)
		}
	}

	return nil
}

func verifyOperationStatuses(networkOs, asserterOs []*types.OperationStatus) error {
	if len(networkOs) != len(asserterOs) {
		return fmt.Errorf("network options operation status length %d, asserter configuration operation status length %d: %w", len(networkOs), len(asserterOs), cliErrs.ErrOperationStatusLengthMismatch)
	}

	sort.Slice(networkOs, func(i, j int) bool {
		return strings.Compare(networkOs[i].Status, networkOs[j].Status) < 0
	})
	sort.Slice(asserterOs, func(i, j int) bool {
		return strings.Compare(asserterOs[i].Status, asserterOs[j].Status) < 0
	})

	for i, networkOperationStatus := range networkOs {
		asserterOperationStatus := asserterOs[i]
		if !reflect.DeepEqual(networkOperationStatus, asserterOperationStatus) {
			return fmt.Errorf("network options operation type %s, asserter configuration operation type %s: %w", types.PrintStruct(networkOperationStatus), types.PrintStruct(asserterOperationStatus), cliErrs.ErrOperationStatusMismatch)
		}
	}

	return nil
}

func verifyErrors(networkErrors, asserterErrors []*types.Error) error {
	if len(networkErrors) != len(asserterErrors) {
		return fmt.Errorf("network options error length %d, asserter configuration error length %d: %w", len(networkErrors), len(asserterErrors), cliErrs.ErrErrorLengthMismatch)
	}

	sort.Slice(networkErrors, func(i, j int) bool {
		return networkErrors[i].Code < networkErrors[j].Code
	})
	sort.Slice(asserterErrors, func(i, j int) bool {
		return asserterErrors[i].Code < asserterErrors[j].Code
	})

	for i, networkError := range networkErrors {
		asserterError := asserterErrors[i]
		if !reflect.DeepEqual(networkError, asserterError) {
			return fmt.Errorf("network options error %s, asserter configuration error %s: %w", types.PrintStruct(networkError), types.PrintStruct(asserterError), cliErrs.ErrErrorMismatch)
		}
	}

	return nil
}
