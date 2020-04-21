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

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// StatelessSyncer contains the logic that orchestrates
// stateless block fetching and reconciliation. The stateless
// syncer is useful for performing a quick check over a range of
// blocks without needed to sync all blocks up to the start of
// the range (a common pattern when debugging). It is important
// to note that the stateless syncer does not support reorgs nor
// does it save where it is on restart.
type StatelessSyncer struct {
	network      *types.NetworkIdentifier
	fetcher      *fetcher.Fetcher
	handler      Handler
	currentIndex int64
}

// NewStateless returns a new Syncer.
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

// SetStartIndex initializes the current block index
// with the genesis block index if it is -1.
func (s *StatelessSyncer) SetStartIndex(
	ctx context.Context,
	startIndex int64,
) error {
	if startIndex != -1 {
		s.currentIndex = startIndex
		return nil
	}

	// Sync from genesis + 1
	networkStatus, err := s.fetcher.NetworkStatusRetry(
		ctx,
		s.network,
		nil,
	)
	if err != nil {
		return err
	}

	// Don't sync genesis block because balance lookup will not
	// work.
	s.currentIndex = networkStatus.GenesisBlockIdentifier.Index + 1
	return nil
}

// SyncRange syncs blocks from startIndex to endIndex, inclusive.
// This function does NOT handle re-orgs. If you want re-org support,
// checkout the StatefulSyncer.
func (s *StatelessSyncer) SyncRange(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	blockMap, err := s.fetcher.BlockRange(ctx, s.network, startIndex, endIndex)
	if err != nil {
		return err
	}

	for i := startIndex; i <= endIndex; i++ {
		block := blockMap[i].Block
		changes, err := BalanceChanges(
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

	s.currentIndex = endIndex + 1

	return nil
}

// CurrentIndex returns the next index to sync.
func (s *StatelessSyncer) CurrentIndex(
	ctx context.Context,
) (int64, error) {
	return s.currentIndex, nil
}

// Network returns the syncer network.
func (s *StatelessSyncer) Network(
	ctx context.Context,
) *types.NetworkIdentifier {
	return s.network
}

// Fetcher returns the syncer fetcher.
func (s *StatelessSyncer) Fetcher(
	ctx context.Context,
) *fetcher.Fetcher {
	return s.fetcher
}
