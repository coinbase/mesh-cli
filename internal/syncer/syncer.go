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
	"log"
	"strings"
	"time"

	"github.com/coinbase/rosetta-validator/internal/logger"
	"github.com/coinbase/rosetta-validator/internal/reconciler"
	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

const (
	// maxSync is the maximum number of blocks
	// to try and sync in a given SyncCycle.
	maxSync = 500
)

// Syncer contains the logic that orchestrates
// block fetching, storage, and reconciliation.
type Syncer struct {
	network    *rosetta.NetworkIdentifier
	storage    *storage.BlockStorage
	fetcher    *fetcher.Fetcher
	logger     *logger.Logger
	reconciler *reconciler.Reconciler
}

// New returns a new Syncer.
func New(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	storage *storage.BlockStorage,
	fetcher *fetcher.Fetcher,
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
) *Syncer {
	return &Syncer{
		network:    network,
		storage:    storage,
		fetcher:    fetcher,
		logger:     logger,
		reconciler: reconciler,
	}
}

// checkReorg determines if the block provided
// has the current head block identifier as its
// parent. If not, it is considered a reorg.
func (s *Syncer) checkReorg(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	block *rosetta.Block,
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
func (s *Syncer) storeBlockBalanceChanges(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	block *rosetta.Block,
	orphan bool,
) ([]*reconciler.AccountAndCurrency, error) {
	modifiedAccounts := make([]*reconciler.AccountAndCurrency, 0)
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			successful, err := s.fetcher.Asserter.OperationSuccessful(op)
			if err != nil {
				// Could only occur if responses not validated
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

			accountAndCurrency := &reconciler.AccountAndCurrency{
				Account:  op.Account,
				Currency: amount.Currency,
			}
			if !reconciler.ContainsAccountAndCurrency(modifiedAccounts, accountAndCurrency) {
				modifiedAccounts = append(modifiedAccounts, accountAndCurrency)
			}

			err = s.storage.UpdateBalance(
				ctx,
				dbTx,
				op.Account,
				amount,
				blockIdentifier,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	return modifiedAccounts, nil
}

// OrphanBlock removes a block from the database and reverts all its balance
// changes.
func (s *Syncer) OrphanBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	blockIdentifier *rosetta.BlockIdentifier,
) ([]*reconciler.AccountAndCurrency, error) {
	log.Printf("Orphaning block %+v\n", blockIdentifier)
	block, err := s.storage.GetBlock(ctx, tx, blockIdentifier)
	if err != nil {
		return nil, err
	}

	err = s.storage.StoreHeadBlockIdentifier(ctx, tx, block.ParentBlockIdentifier)
	if err != nil {
		return nil, err
	}

	modifiedAccounts, err := s.storeBlockBalanceChanges(ctx, tx, block, true)
	if err != nil {
		return nil, err
	}

	err = s.storage.RemoveBlock(ctx, tx, blockIdentifier)
	if err != nil {
		return nil, err
	}

	return modifiedAccounts, nil
}

// AddBlock adds a block to the database and stores all balance changes.
func (s *Syncer) AddBlock(
	ctx context.Context,
	tx storage.DatabaseTransaction,
	block *rosetta.Block,
) ([]*reconciler.AccountAndCurrency, error) {
	log.Printf("Adding block %+v\n", block.BlockIdentifier)
	err := s.storage.StoreBlock(ctx, tx, block)
	if err != nil {
		return nil, err
	}

	err = s.storage.StoreHeadBlockIdentifier(ctx, tx, block.BlockIdentifier)
	if err != nil {
		return nil, err
	}

	modifiedAccounts, err := s.storeBlockBalanceChanges(ctx, tx, block, false)
	if err != nil {
		return nil, err
	}

	return modifiedAccounts, nil
}

// ProcessBlock determines if a block should be added or the current
// head should be orphaned.
func (s *Syncer) ProcessBlock(
	ctx context.Context,
	currIndex int64,
	block *rosetta.Block,
) ([]*reconciler.AccountAndCurrency, int64, error) {
	tx := s.storage.NewDatabaseTransaction(ctx, true)
	defer tx.Discard(ctx)

	reorg, err := s.checkReorg(ctx, tx, block)
	if err != nil {
		return nil, currIndex, err
	}

	var modifiedAccounts []*reconciler.AccountAndCurrency
	var newIndex int64
	if reorg {
		if currIndex == 0 {
			return nil, 0, errors.New("Can't reorg genesis block")
		}

		head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
		if err != nil {
			return nil, currIndex, err
		}

		modifiedAccounts, err = s.OrphanBlock(ctx, tx, head)
		if err != nil {
			return nil, currIndex, err
		}

		newIndex = currIndex - 1
		err = s.logger.BlockStream(ctx, block, true)
		if err != nil {
			log.Printf("Unable to log block %v\n", err)
		}
	} else {
		modifiedAccounts, err = s.AddBlock(ctx, tx, block)
		if err != nil {
			return nil, currIndex, err
		}

		newIndex = currIndex + 1
		err = s.logger.BlockStream(ctx, block, false)
		if err != nil {
			log.Printf("Unable to log block %v\n", err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, currIndex, err
	}

	return modifiedAccounts, newIndex, nil
}

// SyncBlockRange syncs blocks from startIndex to endIndex, inclusive.
// This function handles re-orgs that may occur while syncing.
func (s *Syncer) SyncBlockRange(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	allBlocks := make([]*fetcher.BlockAndLatency, 0)
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
				&rosetta.PartialBlockIdentifier{
					Index: &currIndex,
				},
				fetcher.DefaultElapsedTime,
				fetcher.DefaultRetries,
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

		// Can't return modifiedAccounts without creating new variable
		modifiedAccounts, newIndex, err := s.ProcessBlock(
			ctx,
			currIndex,
			block.Block,
		)
		if err != nil {
			return err
		}

		currIndex = newIndex
		allBlocks = append(allBlocks, block)
		s.reconciler.QueueAccounts(ctx, block.Block.BlockIdentifier.Index, modifiedAccounts)
	}

	return s.logger.BlockLatency(ctx, allBlocks)
}

// SyncCycle is a single iteration of processing up to maxSync blocks.
// SyncCycle is called repeatedly by Sync until there is an error.
func (s *Syncer) SyncCycle(ctx context.Context, printNetwork bool) error {
	networkStatus, err := s.fetcher.NetworkStatusRetry(
		ctx,
		nil,
		fetcher.DefaultElapsedTime,
		fetcher.DefaultRetries,
	)
	if err != nil {
		return err
	}

	if printNetwork {
		err = logger.Network(ctx, networkStatus)
		if err != nil {
			return err
		}
	}

	tx := s.storage.NewDatabaseTransaction(ctx, false)
	defer tx.Discard(ctx)

	head, err := s.storage.GetHeadBlockIdentifier(ctx, tx)
	if err == storage.ErrHeadBlockNotFound {
		head = networkStatus.NetworkStatus.NetworkInformation.GenesisBlockIdentifier
	} else if err != nil {
		return err
	}

	currIndex := head.Index + 1
	endIndex := networkStatus.NetworkStatus.NetworkInformation.CurrentBlockIdentifier.Index
	if endIndex-currIndex > maxSync {
		endIndex = currIndex + maxSync
	}

	if currIndex > endIndex {
		log.Printf("Next block %d > Blockchain Head %d", currIndex, endIndex)
		return nil
	}

	log.Printf("Syncing blocks %d-%d\n", currIndex, endIndex)
	return s.SyncBlockRange(ctx, currIndex, endIndex)
}

// Sync cycles endlessly until there is an error.
func (s *Syncer) Sync(ctx context.Context) error {
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
