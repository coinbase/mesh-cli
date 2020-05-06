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
	"errors"

	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type ReconcilerHelper struct {
	storage *storage.BlockStorage
}

func NewReconcilerHelper(
	storage *storage.BlockStorage,
) *ReconcilerHelper {
	return &ReconcilerHelper{
		storage: storage,
	}
}

func (h *ReconcilerHelper) BlockExists(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	_, err := h.storage.GetBlock(ctx, block)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, storage.ErrBlockNotFound) {
		return false, nil
	}

	return false, err
}

func (h *ReconcilerHelper) CurrentBlock(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return h.storage.GetHeadBlockIdentifier(ctx)
}

func (h *ReconcilerHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	return h.storage.GetBalance(ctx, account, currency, headBlock)
}
