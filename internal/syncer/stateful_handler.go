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

package syncer

import (
	"context"
	"log"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// StatefulHandler logs processed blocks
// and reconciles modified balances.
type StatefulHandler struct {
	logger     *logger.Logger
	reconciler *reconciler.StatefulReconciler
}

// NewStatefulHandler constructs a basic Handler.
func NewStatefulHandler(
	logger *logger.Logger,
	reconciler *reconciler.StatefulReconciler,
) Handler {
	return &StatefulHandler{
		logger:     logger,
		reconciler: reconciler,
	}
}

// BlockProcessed is called by the syncer after each
// block is processed.
func (h *StatefulHandler) BlockProcessed(
	ctx context.Context,
	block *types.Block,
	reorg bool,
	balanceChanges []*storage.BalanceChange,
) error {
	if !reorg {
		log.Printf("Adding block %+v\n", block.BlockIdentifier)
	} else {
		log.Printf("Orphaning block %+v\n", block.BlockIdentifier)
	}
	// Log processed blocks and balance changes
	if err := h.logger.BlockStream(ctx, block, reorg); err != nil {
		return nil
	}

	if err := h.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// Mark accounts for reconciliation
	h.reconciler.QueueAccounts(ctx, block.BlockIdentifier.Index, balanceChanges)

	return nil
}
