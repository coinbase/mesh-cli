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

// Confirm `/network/options` has relevant settings that match the assert configuration serlialized
// and written to `asserterConfigurationPath`
func validateAsserterConfiguration(
	ctx context.Context, f *fetcher.Fetcher, network *types.NetworkIdentifier,
	asserterConfigurationPath string,
) error {
	resp, fetchErr := f.NetworkOptions(ctx, Config.Network, nil)
	if fetchErr != nil {
		return fetchErr.Err // how to return the errors?
	}

	var asserterConfiguration asserter.Configuration
	if err := utils.LoadAndParse(asserterConfigurationPath, &asserterConfiguration); err != nil {
		return err
	}

	return verifyAllowMatch(resp, &asserterConfiguration)
}

func verifyAllowMatch(
	resp *types.NetworkOptionsResponse, configuration *asserter.Configuration,
) error {
	if resp == nil {
		return errors.New("resp nil")
	}
	networkAllow := resp.Allow
	if networkAllow == nil {
		return errors.New("network's allow is nil")
	}
	if configuration == nil {
		return errors.New("configuration nil")
	}

	if err := verifyTimestampStartIndex(
		networkAllow.TimestampStartIndex, configuration.AllowedTimestampStartIndex,
	); err != nil {
		return err
	}

	if err := verifyOperationTypes(
		networkAllow.OperationTypes, configuration.AllowedOperationTypes,
	); err != nil {
		return err
	}

	if err := verifyOperationStatuses(
		networkAllow.OperationStatuses, configuration.AllowedOperationStatuses,
	); err != nil {
		return err
	}

	return verifyErrors(networkAllow.Errors, configuration.AllowedErrors)
}

func verifyTimestampStartIndex(networkTsi *int64, assertTsi int64) error {
	if networkTsi == nil {
		return errors.New("network allow timestamp start index nil")
	}
	if *networkTsi != assertTsi {
		return fmt.Errorf(
			"network and asserter timestamp start index mismatch. %d %d",
			*networkTsi, assertTsi,
		)
	}

	return nil
}

func verifyOperationTypes(networkOt, asserterOt []string) error {
	if len(networkOt) != len(asserterOt) {
		return fmt.Errorf(
			"network and asserter have operation type list size mismatch %v %v",
			networkOt, asserterOt,
		)
	}

	sort.Strings(networkOt)
	sort.Strings(asserterOt)

	for i, networkOperationType := range networkOt {
		asserterOperationType := asserterOt[i]
		if networkOperationType != asserterOperationType {
			return fmt.Errorf(
				"network / asserter operation type mismatch %v %v",
				networkOperationType, asserterOperationType,
			)
		}
	}

	return nil
}

func verifyOperationStatuses(networkOs, asserterOs []*types.OperationStatus) error {
	if len(networkOs) != len(asserterOs) {
		return fmt.Errorf(
			"network and asserter have operation status list size mismatch %v %v",
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
				"network / asserter operation status mismatch %v %v",
				networkOperationStatus, asserterOperationStatus,
			)
		}
	}

	return nil
}

func verifyErrors(networkErrors, asserterErrors []*types.Error) error {
	if len(networkErrors) != len(asserterErrors) {
		return fmt.Errorf(
			"network and asserter have error list size mismatch %v %v",
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
				"network / asserter operation error mismatch %v %v",
				networkError, asserterError,
			)
		}
	}

	return nil
}
