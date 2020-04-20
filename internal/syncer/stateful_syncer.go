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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"

	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// maxSync is the maximum number of blocks
	// to try and sync in a given SyncCycle.
	maxSync = 500
)

// StatefulSyncer contains the logic that orchestrates
// block fetching, storage, and reconciliation.
type StatefulSyncer struct {
	network *types.NetworkIdentifier
	storage *storage.BlockStorage
	fetcher *fetcher.Fetcher
	handler Handler
}

// NewStateful returns a new StatefulSyncer.
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

func calculateBalanceChanges(
	ctx context.Context,
	asserter *asserter.Asserter,
	block *types.Block,
	orphan bool,
) (map[string]*storage.BalanceChange, error) {
	balanceChanges := map[string]*storage.BalanceChange{}
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			successful, err := asserter.OperationSuccessful(op)
			if err != nil {
				// Should only occur if responses not validated
				return nil, err
			}

			if !successful {
				continue
			}

			if op.Account == nil {
				continue
			}

			amount := op.Amount
			blockIdentifier := block.BlockIdentifier
			if orphan {
				if strings.HasPrefix(amount.Value, "-") {
					amount.Value = amount.Value[1:]
				} else {
					amount.Value = "-" + amount.Value
				}

				blockIdentifier = block.ParentBlockIdentifier
			}

			// Merge values by account and currency
			key := fmt.Sprintf("%s:%s",
				string(storage.GetBalanceKey(op.Account)),
				string(storage.GetCurrencyKey(op.Amount.Currency)),
			)

			val, ok := balanceChanges[key]
			if !ok {
				balanceChanges[key] = &storage.BalanceChange{
					Account:  op.Account,
					Currency: op.Amount.Currency,
					NewValue: amount.Value,
					Block:    blockIdentifier,
				}
				continue
			}

			val.NewValue, err = storage.AddStringValues(val.NewValue, amount.Value)
			if err != nil {
				return nil, err
			}

			balanceChanges[key] = val
		}
	}

	return balanceChanges, nil
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
	mergedChanges, err := calculateBalanceChanges(
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
				Value:    change.NewValue,
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

// OrphanBlock removes a block from the database and reverts all its balance
// changes.
func (s *StatefulSyncer) OrphanBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
) ([]*storage.BalanceChange, error) {
	log.Printf("Orphaning block %+v\n", blockIdentifier)
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

// AddBlock adds a block to the database and stores all balance changes.
func (s *StatefulSyncer) AddBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	block *types.Block,
) ([]*storage.BalanceChange, error) {
	log.Printf("Adding block %+v\n", block.BlockIdentifier)
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

// ProcessBlock determines if a block should be added or the current
// head should be orphaned.
func (s *StatefulSyncer) ProcessBlock(
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

		balanceChanges, err = s.OrphanBlock(ctx, tx, head)
		if err != nil {
			return nil, currIndex, false, err
		}
	} else {
		balanceChanges, err = s.AddBlock(ctx, tx, block)
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

// NewHeadIndex reverts all blocks that have
// an index greater than newHeadIndex. This is particularly
// useful when debugging a server implementation because
// you don't need to restart validation from genesis. Instead,
// you can just restart validation at the block immediately
// before any erroneous block.
func (s *StatefulSyncer) NewHeadIndex(
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

		_, err = s.OrphanBlock(ctx, tx, head)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// SyncBlockRange syncs blocks from startIndex to endIndex, inclusive.
// This function handles re-orgs that may occur while syncing as long
// as the genesisIndex is not orphaned.
func (s *StatefulSyncer) SyncBlockRange(
	ctx context.Context,
	genesisIndex int64,
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
			start := time.Now()
			blockValue, err := s.fetcher.BlockRetry(
				ctx,
				s.network,
				&types.PartialBlockIdentifier{
					Index: &currIndex,
				},
			)
			if err != nil {
				return err
			}

			block = &fetcher.BlockAndLatency{
				Block:   blockValue,
				Latency: time.Since(start).Seconds(),
			}
		} else {
			// Anytime we re-fetch an index, we
			// will need to make another call to the node
			// as it is likely in a reorg.
			delete(blockMap, currIndex)
		}

		// Can't return balanceChanges without creating new variable
		balanceChanges, newIndex, reorg, err := s.ProcessBlock(
			ctx,
			genesisIndex,
			currIndex,
			block.Block,
		)
		if err != nil {
			return err
		}

		currIndex = newIndex

		if err := s.handler.BlockProcessed(ctx, block.Block, reorg, balanceChanges); err != nil {
			return err
		}
	}

	return nil
}

// nextSyncableRange returns the next range of indexes to sync
// based on what the last processed block in storage is and
// the contents of the network status response.
func (s *StatefulSyncer) nextSyncableRange(
	ctx context.Context,
	networkStatus *types.NetworkStatusResponse,
) (int64, int64, int64, error) {
	tx := s.storage.NewDatabaseTransaction(ctx, false)
	defer tx.Discard(ctx)

	genesisBlockIdentifier := networkStatus.GenesisBlockIdentifier

	var startIndex int64
	head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
	switch err {
	case nil:
		startIndex = head.Index + 1
	case storage.ErrHeadBlockNotFound:
		head = genesisBlockIdentifier
		startIndex = head.Index
	default:
		return -1, -1, -1, err
	}

	endIndex := networkStatus.CurrentBlockIdentifier.Index
	if endIndex-startIndex > maxSync {
		endIndex = startIndex + maxSync
	}

	return genesisBlockIdentifier.Index, startIndex, endIndex, nil
}

// PrintNetwork pretty prints the types.NetworkStatusResponse to the console.
func PrintNetwork(
	ctx context.Context,
	network *types.NetworkStatusResponse,
) error {
	b, err := json.MarshalIndent(network, "", " ")
	if err != nil {
		return err
	}

	fmt.Println("Network Information: " + string(b))

	return nil
}

// SyncCycle is a single iteration of processing up to maxSync blocks.
// SyncCycle is called repeatedly by Sync until there is an error.
func (s *StatefulSyncer) SyncCycle(ctx context.Context, printNetwork bool) error {
	networkStatus, err := s.fetcher.NetworkStatusRetry(
		ctx,
		s.network,
		nil,
	)
	if err != nil {
		return err
	}

	if printNetwork {
		err = PrintNetwork(ctx, networkStatus)
		if err != nil {
			return err
		}
	}

	genesisIndex, startIndex, endIndex, err := s.nextSyncableRange(ctx, networkStatus)
	if err != nil {
		return err
	}

	if startIndex > endIndex {
		log.Printf("Next block %d > Blockchain Head %d", startIndex, endIndex)
		return nil
	}

	log.Printf("Syncing blocks %d-%d\n", startIndex, endIndex)
	return s.SyncBlockRange(
		ctx,
		genesisIndex,
		startIndex,
		endIndex,
	)
}

// Sync cycles endlessly until there is an error.
func (s *StatefulSyncer) Sync(
	ctx context.Context,
) error {
	printNetwork := true
	for ctx.Err() == nil {
		err := s.SyncCycle(ctx, printNetwork)
		if err != nil {
			return err
		}
		printNetwork = false
	}

	return nil
}
