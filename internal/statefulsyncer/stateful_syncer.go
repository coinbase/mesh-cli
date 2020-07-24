package statefulsyncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/processor"
	"github.com/coinbase/rosetta-cli/internal/storage"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var _ syncer.Handler = (*StatefulSyncer)(nil)

type StatefulSyncer struct {
	blockStorage   *storage.BlockStorage
	counterStorage *storage.CounterStorage
	logger         *logger.Logger
	syncer         *syncer.Syncer
	workers        []storage.BlockWorker
	handlers       []Handler
}

func New(
	ctx context.Context,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	db storage.Database,
	logger *logger.Logger,
	cancel context.CancelFunc,
	workers []storage.BlockWorker,
	handlers []Handler,
) *StatefulSyncer {
	// Load storage backends
	counterStorage := storage.NewCounterStorage(db)
	blockStorage := storage.NewBlockStorage(db)

	// Load in previous blocks into syncer cache to handle reorgs.
	// If previously processed blocks exist in storage, they are fetched.
	// Otherwise, none are provided to the cache (the syncer will not attempt
	// a reorg if the cache is empty).
	pastBlocks := blockStorage.CreateBlockCache(ctx)

	s := &StatefulSyncer{
		blockStorage: blockStorage,
		workers:      workers,
		handlers:     handlers,
	}

	s.syncer = syncer.New(
		network,
		fetcher,
		s,
		cancel,
		pastBlocks,
	)

	return s
}

func (s *StatefulSyncer) Sync(ctx context.Context, startIndex int64, endIndex int64) error {
	return s.syncer.Sync(ctx, startIndex, endIndex)
}

func (s *StatefulSyncer) BlockAdded(ctx context.Context, block *types.Block) error {
	err := s.blockStorage.AddBlock(ctx, block, s.workers)
	if err != nil {
		return fmt.Errorf("%w: unable to add block to storage %s:%d", err, block.BlockIdentifier.Hash, block.BlockIdentifier.Index)
	}

	if err := s.logger.AddBlockStream(ctx, block); err != nil {
		return nil
	}

	// Update Counters
	_, _ = s.counterStorage.Update(ctx, storage.BlockCounter, big.NewInt(1))
	_, _ = s.counterStorage.Update(
		ctx,
		storage.TransactionCounter,
		big.NewInt(int64(len(block.Transactions))),
	)
	opCount := int64(0)
	for _, txn := range block.Transactions {
		opCount += int64(len(txn.Operations))
	}
	_, _ = s.counterStorage.Update(ctx, storage.OperationCounter, big.NewInt(opCount))

	return nil

	// how to get info out of workers to log? (i.e. balance changes if we don't know when commit happens?)
	// i guess this would have to be stored or computed on the fly?
}

func (s *StatefulSyncer) BlockRemoved(ctx context.Context, blockIdentifier *types.BlockIdentifier) error {
	err := s.blockStorage.RemoveBlock(ctx, blockIdentifier, s.workers)
	if err != nil {
		return fmt.Errorf("%w: unable to remove block from storage %s:%d", err, blockIdentifier.Hash, blockIdentifier.Index)
	}

	if err := s.logger.RemoveBlockStream(ctx, blockIdentifier); err != nil {
		return nil
	}

	// Update Counters
	_, _ = s.counterStorage.Update(ctx, storage.OrphanCounter, big.NewInt(1))

	return err
}
