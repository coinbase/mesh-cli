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

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// BaseHandler logs processed blocks
// and reconciles modified balances.
type BaseHandler struct {
	logger     *logger.Logger
	reconciler reconciler.Reconciler
}

// NewBaseHandler constructs a basic Handler.
func NewBaseHandler(
	logger *logger.Logger,
	reconciler reconciler.Reconciler,
) Handler {
	return &BaseHandler{
		logger:     logger,
		reconciler: reconciler,
	}
}

// BlockProcessed is called by the syncer after each
// block is processed.
// TODO: refactor to BlockAdded and BlockRemoved
func (h *BaseHandler) BlockProcessed(
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

	// Mark accounts for reconciliation...this may be
	// blocking
	return h.reconciler.QueueChanges(ctx, block.BlockIdentifier, balanceChanges)
}
