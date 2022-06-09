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

	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	errBlockIdentifier    = errors.New("BlockIdentifier must have both index and hash")
	errTimestamp          = errors.New("Timestamp must be greater than or equal to 0")
	errPeer               = errors.New("Peer should be present and peer id should not be empty")
	errVersion            = errors.New("Version should not be empty")
	errOperationStatus    = errors.New("Operation status should not be empty")
	errOperationTypeEmpty = errors.New("Operation type should not be empty")

	errErrorEmpty        = errors.New("Error object can't be empty")
	errErrorEmptyMessage = errors.New("Error object can't have empty message")
	errErrorNegativeCode = errors.New("Error object can't have negative code")

	errCallMethodEmptyName   = errors.New("Call method name can't be empty")
	errBalanceExemptionEmpty = errors.New("Balance exemption can't be empty")

	errAllowNullPointer = errors.New("Null pointer to Allow object")
)

func validateBlockIdentifier(blockID *types.BlockIdentifier) error {
	if blockID != nil && blockID.Hash != "" && blockID.Index >= 0 {
		return nil
	}

	return errBlockIdentifier
}

func validateTimestamp(time int64) error {
	if time >= 0 {
		return nil
	}

	return errTimestamp
}

func validatePeers(peers []*types.Peer) error {
	for _, p := range peers {
		if p == nil || p.PeerID == "" {
			return errPeer
		}
	}

	return nil
}

func validateVersion(version string) error {
	if version == "" {
		return errVersion
	}

	return nil
}

func validateOperationStatuses(oss []*types.OperationStatus) error {
	for _, os := range oss {
		if os == nil || os.Status == "" {
			return errOperationStatus
		}
	}

	return nil
}

func validateOperationTypes(ots []string) error {
	for _, ot := range ots {
		if ot == "" {
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

		if err.Code < 0 {
			return errErrorNegativeCode
		}

		if err.Message == "" {
			return errErrorEmptyMessage
		}
	}

	return nil
}

func validateCallMethods(methods []string) error {
	for _, m := range methods {
		if m == "" {
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
