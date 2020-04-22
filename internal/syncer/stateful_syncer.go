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
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// StatefulSyncer contains the logic that orchestrates
// block fetching, storage, and reconciliation. The
// stateful syncer is useful for creating an application
// where durability and consistency of data is important.
// The stateful syncer supports re-orgs out-of the box.
type StatefulSyncer struct {
	network      *types.NetworkIdentifier
	storage      *storage.BlockStorage
	fetcher      *fetcher.Fetcher
	handler      Handler
	genesisBlock *types.BlockIdentifier
}

// NewStateful returns a new Syncer.
func NewStateful(
	network *types.NetworkIdentifier,
	storage *storage.BlockStorage,
	fetcher *fetcher.Fetcher,
	handler Handler,
) *StatefulSyncer {
	return &StatefulSyncer{
		network: network,
		storage: storage,
		fetcher: fetcher,
		handler: handler,
	}
}

// SetStartIndex initializes the genesisBlock
// and attempts to set the newHeadIndex.
func (s *StatefulSyncer) SetStartIndex(
	ctx context.Context,
	startIndex int64,
) error {
	networkStatus, err := s.fetcher.NetworkStatusRetry(
		ctx,
		s.network,
		nil,
	)
	if err != nil {
		return err
	}

	s.genesisBlock = networkStatus.GenesisBlockIdentifier

	if startIndex != -1 {
		return s.newHeadIndex(ctx, startIndex)
	}

	return nil
}

// checkReorg determines if the block provided
// has the current head block identifier as its
// parent. If not, it is considered a reorg.
func (s *StatefulSyncer) checkReorg(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	block *types.Block,
) (bool, error) {
	head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
	if err == storage.ErrHeadBlockNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if block.ParentBlockIdentifier.Index != head.Index {
		return false, fmt.Errorf(
			"Got block %d instead of %d",
			block.BlockIdentifier.Index,
			head.Index+1,
		)
	}

	if block.ParentBlockIdentifier.Hash != head.Hash {
		return true, nil
	}

	return false, nil
}

// storeBlockBalanceChanges updates the balance
// of each modified account if the operation affecting
// that account is successful. These modified
// accounts are returned to the reconciler
// for active reconciliation.
func (s *StatefulSyncer) storeBlockBalanceChanges(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	block *types.Block,
	orphan bool,
) ([]*storage.BalanceChange, error) {
	balanceChanges := make([]*storage.BalanceChange, 0)

	// Merge all changes for an account:currency
	mergedChanges, err := BalanceChanges(
		ctx,
		s.fetcher.Asserter,
		block,
		orphan,
	)
	if err != nil {
		return nil, err
	}

	for _, change := range mergedChanges {
		balanceChange, err := s.storage.UpdateBalance(
			ctx,
			dbTx,
			change.Account,
			&types.Amount{
				Value:    change.Difference,
				Currency: change.Currency,
			},
			change.Block,
		)
		if err != nil {
			return nil, err
		}

		balanceChanges = append(balanceChanges, balanceChange)
	}

	return balanceChanges, nil
}

// orphanBlock removes a block from the database and reverts all its balance
// changes.
func (s *StatefulSyncer) orphanBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
) ([]*storage.BalanceChange, error) {
	block, err := s.storage.GetBlock(ctx, tx, blockIdentifier)
	if err != nil {
		return nil, err
	}

	err = s.storage.StoreHeadBlockIdentifier(ctx, tx, block.ParentBlockIdentifier)
	if err != nil {
		return nil, err
	}

	balanceChanges, err := s.storeBlockBalanceChanges(ctx, tx, block, true)
	if err != nil {
		return nil, err
	}

	err = s.storage.RemoveBlock(ctx, tx, blockIdentifier)
	if err != nil {
		return nil, err
	}

	return balanceChanges, nil
}

// addBlock adds a block to the database and stores all balance changes.
func (s *StatefulSyncer) addBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	block *types.Block,
) ([]*storage.BalanceChange, error) {
	err := s.storage.StoreBlock(ctx, tx, block)
	if err != nil {
		return nil, err
	}

	err = s.storage.StoreHeadBlockIdentifier(ctx, tx, block.BlockIdentifier)
	if err != nil {
		return nil, err
	}

	balanceChanges, err := s.storeBlockBalanceChanges(ctx, tx, block, false)
	if err != nil {
		return nil, err
	}

	return balanceChanges, nil
}

// processBlock determines if a block should be added or the current
// head should be orphaned.
func (s *StatefulSyncer) processBlock(
	ctx context.Context,
	genesisIndex int64,
	currIndex int64,
	block *types.Block,
) ([]*storage.BalanceChange, int64, bool, error) {
	tx := s.storage.NewDatabaseTransaction(ctx, true)
	defer tx.Discard(ctx)

	reorg, err := s.checkReorg(ctx, tx, block)
	if err != nil {
		return nil, currIndex, false, err
	}

	var balanceChanges []*storage.BalanceChange
	var newIndex int64
	if reorg {
		newIndex = currIndex - 1
		if newIndex == genesisIndex {
			return nil, 0, false, errors.New("cannot orphan genesis block")
		}

		head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
		if err != nil {
			return nil, currIndex, false, err
		}

		balanceChanges, err = s.orphanBlock(ctx, tx, head)
		if err != nil {
			return nil, currIndex, false, err
		}
	} else {
		balanceChanges, err = s.addBlock(ctx, tx, block)
		if err != nil {
			return nil, currIndex, false, err
		}

		newIndex = currIndex + 1
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, currIndex, false, err
	}

	return balanceChanges, newIndex, reorg, nil
}

// newHeadIndex reverts all blocks that have
// an index greater than newHeadIndex. This is particularly
// useful when debugging a server implementation because
// you don't need to restart validation from genesis. Instead,
// you can just restart validation at the block immediately
// before any erroneous block.
func (s *StatefulSyncer) newHeadIndex(
	ctx context.Context,
	newHeadIndex int64,
) error {
	tx := s.storage.NewDatabaseTransaction(ctx, true)
	defer tx.Discard(ctx)

	for {
		head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
		if err == storage.ErrHeadBlockNotFound {
			return fmt.Errorf(
				"cannot start syncing at %d, have not yet processed any blocks",
				newHeadIndex,
			)
		} else if err != nil {
			return err
		}

		if head.Index < newHeadIndex {
			return fmt.Errorf(
				"cannot start syncing at %d, have only processed %d blocks",
				newHeadIndex,
				head.Index,
			)
		}

		if head.Index == newHeadIndex {
			break
		}

		_, err = s.orphanBlock(ctx, tx, head)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// SyncRange syncs blocks from startIndex to endIndex, inclusive.
// This function handles re-orgs that may occur while syncing as long
// as the genesisIndex is not orphaned.
func (s *StatefulSyncer) SyncRange(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	blockMap, err := s.fetcher.BlockRange(ctx, s.network, startIndex, endIndex)
	if err != nil {
		return err
	}

	currIndex := startIndex
	for currIndex <= endIndex {
		block, ok := blockMap[currIndex]
		if !ok { // could happen in a reorg
			block, err = s.fetcher.BlockRetry(
				ctx,
				s.network,
				&types.PartialBlockIdentifier{
					Index: &currIndex,
				},
			)
			if err != nil {
				return err
			}
		} else {
			// Anytime we re-fetch an index, we
			// will need to make another call to the node
			// as it is likely in a reorg.
			delete(blockMap, currIndex)
		}

		// Can't return balanceChanges without creating new variable
		balanceChanges, newIndex, reorg, err := s.processBlock(
			ctx,
			s.genesisBlock.Index,
			currIndex,
			block,
		)
		if err != nil {
			return err
		}

		currIndex = newIndex

		if err := s.handler.BlockProcessed(ctx, block, reorg, balanceChanges); err != nil {
			return err
		}
	}

	return nil
}

// CurrentIndex returns the next index to sync.
func (s *StatefulSyncer) CurrentIndex(
	ctx context.Context,
) (int64, error) {
	tx := s.storage.NewDatabaseTransaction(ctx, false)
	defer tx.Discard(ctx)

	var currentIndex int64
	head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
	switch err {
	case nil:
		currentIndex = head.Index + 1
	case storage.ErrHeadBlockNotFound:
		head = s.genesisBlock
		currentIndex = head.Index
	default:
		return -1, err
	}

	return currentIndex, nil
}

// Network returns the syncer network.
func (s *StatefulSyncer) Network(
	ctx context.Context,
) *types.NetworkIdentifier {
	return s.network
}

// Fetcher returns the syncer fetcher.
func (s *StatefulSyncer) Fetcher(
	ctx context.Context,
) *fetcher.Fetcher {
	return s.fetcher
}
