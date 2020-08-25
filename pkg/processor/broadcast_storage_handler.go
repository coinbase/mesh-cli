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
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-cli/configuration"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ storage.BroadcastStorageHandler = (*BroadcastStorageHandler)(nil)

// BroadcastStorageHandler is invoked whenever a block is added
// or removed from block storage so that balance changes
// can be sent to other functions (ex: reconciler).
type BroadcastStorageHandler struct {
	config         *configuration.Configuration
	counterStorage *storage.CounterStorage
	coordinator    *coordinator.Coordinator
	parser         *parser.Parser
}

// NewBroadcastStorageHandler returns a new *BroadcastStorageHandler.
func NewBroadcastStorageHandler(
	config *configuration.Configuration,
	counterStorage *storage.CounterStorage,
	coordinator *coordinator.Coordinator,
	parser *parser.Parser,
) *BroadcastStorageHandler {
	return &BroadcastStorageHandler{
		config:         config,
		counterStorage: counterStorage,
		coordinator:    coordinator,
		parser:         parser,
	}
}

// TransactionConfirmed is called when a transaction is observed on-chain for the
// last time at a block height < current block height - confirmationDepth.
func (h *BroadcastStorageHandler) TransactionConfirmed(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	identifier string,
	blockIdentifier *types.BlockIdentifier,
	transaction *types.Transaction,
	intent []*types.Operation,
) error {
	if err := h.parser.ExpectedOperations(intent, transaction.Operations, false, true); err != nil {
		return fmt.Errorf("%w: confirmed transaction did not match intent", err)
	}

	_, _ = h.counterStorage.UpdateTransactional(ctx, dbTx, storage.TransactionsConfirmedCounter, big.NewInt(1))

	if err := h.coordinator.BroadcastComplete(
		ctx,
		dbTx,
		identifier,
		transaction,
	); err != nil {
		return fmt.Errorf("%w: coordinator could not handle transaction", err)
	}

	return nil
}

// TransactionStale is called when a transaction has not yet been
// seen on-chain and is considered stale. This occurs when
// current block height - last broadcast > staleDepth.
func (h *BroadcastStorageHandler) TransactionStale(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	identifier string,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	_, _ = h.counterStorage.UpdateTransactional(ctx, dbTx, storage.StaleBroadcastsCounter, big.NewInt(1))

	return nil
}

// BroadcastFailed is called when another transaction broadcast would
// put it over the provided broadcast limit.
func (h *BroadcastStorageHandler) BroadcastFailed(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	identifier string,
	transactionIdentifier *types.TransactionIdentifier,
	intent []*types.Operation,
) error {
	_, _ = h.counterStorage.UpdateTransactional(ctx, dbTx, storage.FailedBroadcastsCounter, big.NewInt(1))

	if err := h.coordinator.BroadcastComplete(
		ctx,
		dbTx,
		identifier,
		nil,
	); err != nil {
		return fmt.Errorf("%w: coordinator could not handle transaction", err)
	}

	if h.config.Construction.IgnoreBroadcastFailures {
		return nil
	}

	return fmt.Errorf(
		"broadcast failed for transaction %s with intent %s",
		transactionIdentifier.Hash,
		types.PrettyPrintStruct(intent),
	)
}
