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

package storage

import (
	"context"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ BroadcastStorageHelper = (*MockBroadcastStorageHelper)(nil)

type MockBroadcastStorageHelper struct{}

func (m *MockBroadcastStorageHelper) CurrentBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return nil, nil
}

func (m *MockBroadcastStorageHelper) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
) (*types.BlockIdentifier, *types.Transaction, error) {
	return nil, nil, nil
}

func (m *MockBroadcastStorageHelper) BroadcastTransaction(
	ctx context.Context,
	payload string,
) (*types.TransactionIdentifier, error) {
	return nil, nil
}
