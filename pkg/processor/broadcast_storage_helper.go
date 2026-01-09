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

	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ modules.BroadcastStorageHelper = (*BroadcastStorageHelper)(nil)

// BroadcastStorageHelper implements the storage.Helper
// interface.
type BroadcastStorageHelper struct {
	network      *types.NetworkIdentifier
	blockStorage *modules.BlockStorage
	fetcher      *fetcher.Fetcher
}

// NewBroadcastStorageHelper returns a new BroadcastStorageHelper.
func NewBroadcastStorageHelper(
	network *types.NetworkIdentifier,
	blockStorage *modules.BlockStorage,
	fetcher *fetcher.Fetcher,
) *BroadcastStorageHelper {
	return &BroadcastStorageHelper{
		network:      network,
		blockStorage: blockStorage,
		fetcher:      fetcher,
	}
}

// AtTip is called before transaction broadcast to determine if we are at tip.
func (h *BroadcastStorageHelper) AtTip(
	ctx context.Context,
	tipDelay int64,
) (bool, error) {
	atTip, _, err := utils.CheckStorageTip(ctx, h.network, tipDelay, h.fetcher, h.blockStorage)
	if err != nil {
		return false, fmt.Errorf("failed to check storage tip: %w", err)
	}

	return atTip, nil
}

// CurrentBlockIdentifier is called before transaction broadcast and is used
// to determine if a transaction broadcast is stale.
func (h *BroadcastStorageHelper) CurrentBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	blockIdentifier, err := h.blockStorage.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get head block identifier: %w", err)
	}

	return blockIdentifier, nil
}

// FindTransaction looks for the provided TransactionIdentifier in processed
// blocks and returns the block identifier containing the most recent sighting
// and the transaction seen in that block.
//
// HOTFIX: For TON, also checks metadata.in_message_hash to match transactions
// since TON's on-chain tx hash differs from the submitted message hash.
func (h *BroadcastStorageHelper) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn database.Transaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	// First, try normal lookup by transaction hash
	newestBlock, transaction, err := h.blockStorage.FindTransaction(ctx, transactionIdentifier, txn)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to perform transaction search for transaction %s: %w", types.PrintStruct(transactionIdentifier), err)
	}

	// If found via normal lookup, return it
	if newestBlock != nil {
		return newestBlock, transaction, nil
	}

	// HOTFIX: If not found, search recent blocks for a transaction where
	// metadata.in_message_hash matches the hash we're looking for (TON specific)
	targetHash := transactionIdentifier.Hash
	foundBlock, foundTx := h.findTransactionByInMessageHash(ctx, targetHash)
	if foundBlock != nil && foundTx != nil {
		fmt.Printf("HOTFIX: Found transaction via in_message_hash match: target=%s, actual_tx_hash=%s\n",
			targetHash, foundTx.TransactionIdentifier.Hash)
		return foundBlock, foundTx, nil
	}

	return nil, nil, nil
}

// findTransactionByInMessageHash searches recent blocks for a transaction
// where metadata.in_message_hash matches the target hash.
// This is a HOTFIX for TON where on-chain tx hash differs from submitted message hash.
func (h *BroadcastStorageHelper) findTransactionByInMessageHash(
	ctx context.Context,
	targetHash string,
) (*types.BlockIdentifier, *types.Transaction) {
	// Get current head block
	headBlock, err := h.blockStorage.GetHeadBlockIdentifier(ctx)
	if err != nil || headBlock == nil {
		return nil, nil
	}

	// Search the last N blocks (configurable depth for hotfix)
	const searchDepth = 100

	for i := int64(0); i < searchDepth; i++ {
		blockIndex := headBlock.Index - i
		if blockIndex < 0 {
			break
		}

		block, err := h.blockStorage.GetBlock(ctx, &types.PartialBlockIdentifier{Index: &blockIndex})
		if err != nil || block == nil {
			continue
		}

		// Check each transaction in the block
		for _, tx := range block.Transactions {
			if tx.Metadata == nil {
				continue
			}

			// Check if in_message_hash matches our target
			if inMsgHash, ok := tx.Metadata["in_message_hash"]; ok {
				if hashStr, ok := inMsgHash.(string); ok && hashStr == targetHash {
					return block.BlockIdentifier, tx
				}
			}
		}
	}

	return nil, nil
}

// BroadcastTransaction broadcasts a transaction to a Rosetta implementation
// and returns the *types.TransactionIdentifier returned by the implementation.
func (h *BroadcastStorageHelper) BroadcastTransaction(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	networkTransaction string,
) (*types.TransactionIdentifier, error) {
	transactionIdentifier, _, fetchErr := h.fetcher.ConstructionSubmit(
		ctx,
		networkIdentifier,
		networkTransaction,
	)
	if fetchErr != nil {
		return nil, fmt.Errorf("unable to broadcast transaction %s: %w", networkTransaction, fetchErr.Err)
	}

	return transactionIdentifier, nil
}
