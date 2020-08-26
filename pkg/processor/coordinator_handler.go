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

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ coordinator.Handler = (*CoordinatorHandler)(nil)

// CoordinatorHandler is invoked by the Coordinator
// when addresses are created or transactions are created.
type CoordinatorHandler struct {
	balanceStorageHelper *BalanceStorageHelper

	counterStorage *storage.CounterStorage
}

// NewCoordinatorHandler returns a new
// *CoordinatorHandler.
func NewCoordinatorHandler(
	counterStorage *storage.CounterStorage,
) *CoordinatorHandler {
	return &CoordinatorHandler{
		counterStorage: counterStorage,
	}
}

// TransactionCreated increments the TransactionsCreatedCounter in
// CounterStorage.
func (h *CoordinatorHandler) TransactionCreated(
	ctx context.Context,
	jobIdentifier string,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	_, _ = h.counterStorage.Update(
		ctx,
		storage.TransactionsCreatedCounter,
		big.NewInt(1),
	)

	return nil
}
