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
	"reflect"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// maxSync is the maximum number of blocks
	// to try and sync in a given SyncCycle.
	maxSync = 1000
)

// SyncHandler is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync processor.
type SyncHandler interface {
	BlockAdded(
		ctx context.Context,
		block *types.Block,
	) error

	BlockRemoved(
		ctx context.Context,
		block *types.Block,
	) error
}

type Syncer struct {
	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher
	handler SyncHandler
	cancel  context.CancelFunc

	// Used to keep track of sync state
	genesisBlock *types.BlockIdentifier
	currentBlock *types.BlockIdentifier
}

func NewSyncer(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	handler SyncHandler,
	cancel context.CancelFunc,
) *Syncer {
	return &Syncer{
		network: network,
		fetcher: fetcher,
		handler: handler,
		cancel:  cancel,
	}
}

func (s *Syncer) setStart(
	ctx context.Context,
	index int64,
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

	if index != -1 {
		// Get block at index
		block, err := s.fetcher.BlockRetry(ctx, s.network, &types.PartialBlockIdentifier{Index: &index})
		if err != nil {
			return err
		}

		s.currentBlock = block.BlockIdentifier
		return nil
	}

	s.currentBlock = networkStatus.GenesisBlockIdentifier
	return nil
}

func (s *Syncer) head(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	if s.currentBlock == nil {
		return nil, errors.New("start block not set")
	}

	return s.currentBlock, nil
}

// nextSyncableRange returns the next range of indexes to sync
// based on what the last processed block in storage is and
// the contents of the network status response.
func (s *Syncer) nextSyncableRange(
	ctx context.Context,
	endIndex int64,
) (int64, int64, bool, error) {
	head, err := s.head(ctx)
	if err != nil {
		return -1, -1, false, fmt.Errorf("%w: unable to get current head", err)
	}

	if endIndex == -1 {
		networkStatus, err := s.fetcher.NetworkStatusRetry(
			ctx,
			s.network,
			nil,
		)
		if err != nil {
			return -1, -1, false, fmt.Errorf("%w: unable to get network status", err)
		}

		return head.Index, networkStatus.CurrentBlockIdentifier.Index, false, nil
	}

	if head.Index >= endIndex {
		return -1, -1, true, nil
	}

	return head.Index, endIndex, false, nil
}

func (s *Syncer) removeBlock(
	ctx context.Context,
	block *types.Block,
) (bool, error) {
	// Get current block
	head, err := s.head(ctx)
	if err != nil {
		return false, fmt.Errorf("%w: unable to get current head", err)
	}

	// Ensure processing correct index
	if block.ParentBlockIdentifier.Index != head.Index {
		return false, fmt.Errorf(
			"Got block %d instead of %d",
			block.BlockIdentifier.Index,
			head.Index+1,
		)
	}

	// Check if block parent is head
	if !reflect.DeepEqual(block.ParentBlockIdentifier, head) {
		return true, nil
	}

	return false, nil
}

func (s *Syncer) syncRange(
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

		shouldRemove, err := s.removeBlock(ctx, block)
		if err != nil {
			return err
		}

		if shouldRemove {
			currIndex--
			err = s.handler.BlockRemoved(ctx, block)
		} else {
			currIndex++
			err = s.handler.BlockAdded(ctx, block)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Sync cycles endlessly until there is an error
// or the requested range is synced.
func (s *Syncer) Sync(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	defer s.cancel()

	if err := s.setStart(ctx, startIndex); err != nil {
		return fmt.Errorf("%w: unable to set start index", err)
	}

	for {
		rangeStart, rangeEnd, halt, err := s.nextSyncableRange(
			ctx,
			endIndex,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to get next syncable range", err)
		}
		if halt {
			break
		}

		if rangeEnd-rangeStart > maxSync {
			rangeEnd = rangeStart + maxSync
		}

		log.Printf("Syncing %d-%d\n", rangeStart, rangeEnd)

		err = s.syncRange(ctx, rangeStart, rangeEnd)
		if err != nil {
			return fmt.Errorf("%w: unable to sync range %d-%d", err, rangeStart, rangeEnd)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	log.Printf("Finished syncing %d-%d\n", startIndex, endIndex)
	return nil
}
