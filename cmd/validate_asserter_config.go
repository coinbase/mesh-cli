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
	"errors"
	"fmt"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
	"reflect"
	"sort"
	"strings"
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
		return fmt.Errorf("%w: failure loading / parsing asserter-configuration-file", err)
	}

	resp, fetchErr := f.NetworkOptions(ctx, network, nil)
	if fetchErr != nil {
		return fmt.Errorf("%w: failure getting /network/options", fetchErr.Err)
	}

	return validateNetworkAndAsserterAllowMatch(resp.Allow, &asserterConfiguration)
}

func validateNetworkAndAsserterAllowMatch(
	networkAllow *types.Allow, asserterConfiguration *asserter.Configuration,
) error {
	if networkAllow == nil {
		return errors.New("/network/options object's Allow is nil")
	}
	if asserterConfiguration == nil {
		return errors.New("asserter-configuration-file object is nil")
	}

	if err := verifyTimestampStartIndex(
		networkAllow.TimestampStartIndex, asserterConfiguration.AllowedTimestampStartIndex,
	); err != nil {
		return err
	}

	if err := verifyOperationTypes(
		networkAllow.OperationTypes, asserterConfiguration.AllowedOperationTypes,
	); err != nil {
		return err
	}

	if err := verifyOperationStatuses(
		networkAllow.OperationStatuses, asserterConfiguration.AllowedOperationStatuses,
	); err != nil {
		return err
	}

	return verifyErrors(networkAllow.Errors, asserterConfiguration.AllowedErrors)
}

func verifyTimestampStartIndex(networkTsi *int64, assertTsi int64) error {
	var networkTsiVal int64 = 1
	if networkTsi != nil { // This field is optional and defaults to all allowed
		networkTsiVal = *networkTsi
	}
	if networkTsiVal != assertTsi {
		return fmt.Errorf(
			"/network/options / asserter-configuration-file timestamp start index mismatch. %d %d",
			networkTsiVal, assertTsi,
		)
	}

	return nil
}

func verifyOperationTypes(networkOt, asserterOt []string) error {
	if len(networkOt) != len(asserterOt) {
		return fmt.Errorf(
			"/network/options / asserter-configuration-file operation types length mismatch %+v "+
				"%+v",
			networkOt, asserterOt,
		)
	}

	sort.Strings(networkOt)
	sort.Strings(asserterOt)

	for i, networkOperationType := range networkOt {
		asserterOperationType := asserterOt[i]
		if networkOperationType != asserterOperationType {
			return fmt.Errorf(
				"/network/options / asserter-configuration-file operation type mismatch %+v "+
				"%+v\nnetwork operation types: %+v\nasserter operation types: %+v",
				networkOperationType, asserterOperationType, networkOt, asserterOt,
			)
		}
	}

	return nil
}

func verifyOperationStatuses(networkOs, asserterOs []*types.OperationStatus) error {
	if len(networkOs) != len(asserterOs) {
		return fmt.Errorf(
			"/network/options / asserter-configuration-file operation statuses length mismatch "+
				"%+v %+v",
			networkOs, asserterOs,
		)
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
			return fmt.Errorf(
				"/network/options / asserter-configuration-file operation status mismatch %+v "+
					"%+v\nnetwork operation statuses: %+v\nasserter operation statuses: %+v",
				networkOperationStatus, asserterOperationStatus, networkOs, asserterOs,
			)
		}
	}

	return nil
}

func verifyErrors(networkErrors, asserterErrors []*types.Error) error {
	if len(networkErrors) != len(asserterErrors) {
		return fmt.Errorf(
			"/network/options / asserter-configuration-file errors length mismatch %+v %+v",
			networkErrors, asserterErrors,
		)
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
			return fmt.Errorf(
				"/network/options / asserter-configuration-file error mismatch %+v %+v\n"+
					"network errors: %+v\nasserter errors: %+v",
				networkError, asserterError, networkErrors, asserterErrors,
			)
		}
	}

	return nil
}
