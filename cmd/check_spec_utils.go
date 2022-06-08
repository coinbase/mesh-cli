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
	errBlockIdentifier = errors.New("BlockIdentifier must have both index and hash")
)

func verifyBlockIdentifier(blockID *types.BlockIdentifier) error {
	if blockID != nil && blockID.Hash != "" && blockID.Index >= 0 {
		return nil
	}

	return errBlockIdentifier
}
