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

package processor

import (
	"context"
	"math/big"

	"github.com/slowboat0/rosetta-cli/pkg/constructor"
	"github.com/slowboat0/rosetta-cli/pkg/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ constructor.Handler = (*ConstructorHandler)(nil)

// ConstructorHandler is invoked by the Constructor
// when addresses are created or transactions are created.
type ConstructorHandler struct {
	balanceStorageHelper *BalanceStorageHelper

	counterStorage *storage.CounterStorage
}

// NewConstructorHandler returns a new
// *ConstructorHandler.
func NewConstructorHandler(
	balanceStorageHelper *BalanceStorageHelper,
	counterStorage *storage.CounterStorage,
) *ConstructorHandler {
	return &ConstructorHandler{
		balanceStorageHelper: balanceStorageHelper,
		counterStorage:       counterStorage,
	}
}

// AddressCreated adds an address to balance tracking.
func (h *ConstructorHandler) AddressCreated(ctx context.Context, address string) error {
	h.balanceStorageHelper.AddInterestingAddress(address)

	_, _ = h.counterStorage.Update(ctx, storage.AddressesCreatedCounter, big.NewInt(1))

	return nil
}

// TransactionCreated increments the TransactionsCreatedCounter in
// CounterStorage.
func (h *ConstructorHandler) TransactionCreated(
	ctx context.Context,
	sender string,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	_, _ = h.counterStorage.Update(
		ctx,
		storage.TransactionsCreatedCounter,
		big.NewInt(1),
	)

	return nil
}
