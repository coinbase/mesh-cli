package syncer

import (
	"context"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// StatelessSyncer contains the logic that orchestrates
// stateless block fetching and reconciliation.
type StatelessSyncer struct {
	network *types.NetworkIdentifier
	fetcher *fetcher.Fetcher
	handler Handler
}

// NewStateless returns a new StatelessSyncer.
func NewStateless(
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	handler Handler,
) *StatelessSyncer {
	return &StatelessSyncer{
		network: network,
		fetcher: fetcher,
		handler: handler,
	}
}

func (s *StatelessSyncer) SyncRange(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	blockMap, err := s.fetcher.BlockRange(ctx, s.network, startIndex, endIndex)
	if err != nil {
		return err
	}

	// TODO: support reorgs
	for i := startIndex; i < endIndex; i++ {
		block := blockMap[i].Block
		changes, err := calculateBalanceChanges(
			ctx,
			s.fetcher.Asserter,
			block,
			false,
		)
		if err != nil {
			return err
		}

		err = s.handler.BlockProcessed(
			ctx,
			block,
			false,
			changes,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *StatelessSyncer) nextSyncableRange(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) (int64, int64, bool, error) {
	if startIndex >= endIndex && startIndex != -1 && endIndex != -1 {
		return -1, -1, true, nil
	}

	networkStatus, err := s.fetcher.NetworkStatusRetry(
		ctx,
		s.network,
		nil,
	)
	if err != nil {
		return -1, -1, false, err
	}

	if startIndex > networkStatus.CurrentBlockIdentifier.Index {
		return -1, -1, false, fmt.Errorf(
			"start index %d > current block index %d",
			startIndex,
			networkStatus.CurrentBlockIdentifier.Index,
		)
	}

	if startIndex == -1 {
		// Don't sync genesis block because balance lookup will not
		// work.
		// TODO: figure out some way to do this (could have a hook in handler)
		startIndex = networkStatus.GenesisBlockIdentifier.Index + 1
	}

	if endIndex == -1 {
		endIndex = networkStatus.CurrentBlockIdentifier.Index
	}

	if endIndex-startIndex > maxSync {
		endIndex = startIndex + maxSync
	}

	log.Printf("Syncing %d-%d\n", startIndex, endIndex)
	return startIndex, endIndex, false, nil
}

func (s *StatelessSyncer) Sync(
	ctx context.Context,
	cancel context.CancelFunc,
	startIndex int64,
	endIndex int64,
) error {
	defer cancel()

	currIndex := startIndex
	for ctx.Err() == nil {
		newCurrIndex, stopIndex, halt, err := s.nextSyncableRange(
			ctx,
			currIndex,
			endIndex,
		)
		if err != nil {
			return err
		}
		if halt {
			break
		}

		err = s.SyncRange(ctx, newCurrIndex, stopIndex)
		if err != nil {
			return err
		}

		currIndex = stopIndex
	}
	log.Printf("Finished sycning %d-%d\n", startIndex, endIndex)
	return nil
}
