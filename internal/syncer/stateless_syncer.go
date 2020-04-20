package syncer

import (
	"context"

	"github.com/coinbase/rosetta-validator/internal/storage"

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
	for _, block := range blockMap {
		changes, err := calculateBalanceChanges(
			ctx,
			s.fetcher.Asserter,
			block.Block,
			false,
		)
		if err != nil {
			return err
		}

		allChanges := []*storage.BalanceChange{}
		for _, change := range changes {
			allChanges = append(allChanges, change)
		}

		err = s.handler.BlockProcessed(
			ctx,
			block.Block,
			false,
			allChanges,
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
	if startIndex >= endIndex {
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

	if startIndex == -1 {
		startIndex = networkStatus.GenesisBlockIdentifier.Index
	}

	if endIndex == -1 {
		endIndex = networkStatus.CurrentBlockIdentifier.Index
	}

	if endIndex-startIndex > maxSync {
		endIndex = startIndex + maxSync
	}

	return startIndex, endIndex, false, nil
}

func (s *StatelessSyncer) Sync(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	for ctx.Err() == nil {
		startIndex, stopIndex, halt, err := s.nextSyncableRange(
			ctx,
			startIndex,
			endIndex,
		)
		if err != nil {
			return err
		}
		if halt {
			return nil
		}

		err = s.SyncRange(ctx, startIndex, stopIndex)
		if err != nil {
			return err
		}

		startIndex = stopIndex + 1
	}
	return nil
}
