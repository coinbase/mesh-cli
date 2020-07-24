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

package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// headBlockKey is used to lookup the head block identifier.
	// The head block is the block with the largest index that is
	// not orphaned.
	headBlockKey = "head-block"

	// blockNamespace is prepended to any stored block.
	blockNamespace = "block"

	// blockHashNamespace is prepended to any stored block hash.
	// We cannot just use the stored block key to lookup whether
	// a hash has been used before because it is concatenated
	// with the index of the stored block.
	blockHashNamespace = "block-hash"

	// transactionHashNamespace is prepended to any stored
	// transaction hash.
	transactionHashNamespace = "transaction-hash"
)

var (
	// ErrHeadBlockNotFound is returned when there is no
	// head block found in BlockStorage.
	ErrHeadBlockNotFound = errors.New("head block not found")

	// ErrBlockNotFound is returned when a block is not
	// found in BlockStorage.
	ErrBlockNotFound = errors.New("block not found")

	// ErrDuplicateBlockHash is returned when a block hash
	// cannot be stored because it is a duplicate.
	ErrDuplicateBlockHash = errors.New("duplicate block hash")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("duplicate transaction hash")

	// ErrDuplicateKey is returned when trying to store a key that
	// already exists (this is used by storeHash).
	ErrDuplicateKey = errors.New("duplicate key")
)

func getHeadBlockKey() []byte {
	return []byte(headBlockKey)
}

func getBlockKey(blockIdentifier *types.BlockIdentifier) []byte {
	return []byte(
		fmt.Sprintf("%s/%s/%d", blockNamespace, blockIdentifier.Hash, blockIdentifier.Index),
	)
}

func getBlockHashKey(hash string) []byte {
	return []byte(fmt.Sprintf("%s/%s", blockHashNamespace, hash))
}

func getTransactionHashKey(blockHash string, transactionHash string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", transactionHashNamespace, blockHash, transactionHash))
}

// BlockWorker is an interface that allows for work
// to be done while a block is added/removed from storage
// in the same database transaction as the change.
type BlockWorker interface {
	AddingBlock(context.Context, *types.Block, DatabaseTransaction) (CommitWorker, error)
	RemovingBlock(context.Context, *types.Block, DatabaseTransaction) (CommitWorker, error)
}

// CommitWorker is returned by a BlockWorker to be called after
// changes have been committed. It is common to put logging activities
// in here (that shouldn't be printed until the block is committed).
type CommitWorker func(context.Context) error

// BlockStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type BlockStorage struct {
	db Database

	workers []BlockWorker
}

// NewBlockStorage returns a new BlockStorage.
func NewBlockStorage(
	db Database,
) *BlockStorage {
	return &BlockStorage{
		db: db,
	}
}

func (b *BlockStorage) Initialize(workers []BlockWorker) {
	b.workers = workers
}

// GetHeadBlockIdentifier returns the head block identifier,
// if it exists.
func (b *BlockStorage) GetHeadBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	exists, block, err := transaction.Get(ctx, getHeadBlockKey())
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrHeadBlockNotFound
	}

	var blockIdentifier types.BlockIdentifier
	err = decode(block, &blockIdentifier)
	if err != nil {
		return nil, err
	}

	return &blockIdentifier, nil
}

// StoreHeadBlockIdentifier stores a block identifier
// or returns an error.
func (b *BlockStorage) StoreHeadBlockIdentifier(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
) error {
	buf, err := encode(blockIdentifier)
	if err != nil {
		return err
	}

	return transaction.Set(ctx, getHeadBlockKey(), buf)
}

// GetBlock returns a block, if it exists.
func (b *BlockStorage) GetBlock(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) (*types.Block, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	exists, block, err := transaction.Get(ctx, getBlockKey(blockIdentifier))
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w %+v", ErrBlockNotFound, blockIdentifier)
	}

	var rosettaBlock types.Block
	err = decode(block, &rosettaBlock)
	if err != nil {
		return nil, err
	}

	return &rosettaBlock, nil
}

// AddBlock stores a block or returns an error.
func (b *BlockStorage) AddBlock(
	ctx context.Context,
	block *types.Block,
) error {
	transaction := b.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	buf, err := encode(block)
	if err != nil {
		return err
	}

	// Store block
	err = transaction.Set(ctx, getBlockKey(block.BlockIdentifier), buf)
	if err != nil {
		return err
	}

	if err = b.StoreHeadBlockIdentifier(ctx, transaction, block.BlockIdentifier); err != nil {
		return err
	}

	// Store block hash
	blockHashKey := getBlockHashKey(block.BlockIdentifier.Hash)
	err = b.storeHash(ctx, transaction, blockHashKey)
	if errors.Is(err, ErrDuplicateKey) {
		return fmt.Errorf(
			"%w %s",
			ErrDuplicateBlockHash,
			block.BlockIdentifier.Hash,
		)
	} else if err != nil {
		return fmt.Errorf("%w: unable to store block hash", err)
	}

	// Store all transaction hashes
	for _, txn := range block.Transactions {
		transactionHashKey := getTransactionHashKey(
			block.BlockIdentifier.Hash,
			txn.TransactionIdentifier.Hash,
		)
		err = b.storeHash(ctx, transaction, transactionHashKey)
		if errors.Is(err, ErrDuplicateKey) {
			return fmt.Errorf(
				"%w transaction %s appears multiple times in block %s",
				ErrDuplicateTransactionHash,
				txn.TransactionIdentifier.Hash,
				block.BlockIdentifier.Hash,
			)
		} else if err != nil {
			return fmt.Errorf("%w: unable to store transaction hash", err)
		}
	}

	return b.callWorkersAndCommit(ctx, block, transaction, true)
}

// RemoveBlock removes a block or returns an error.
// RemoveBlock also removes the block hash and all
// its transaction hashes to not break duplicate
// detection. This is called within a re-org.
func (b *BlockStorage) RemoveBlock(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) error {
	block, err := b.GetBlock(ctx, blockIdentifier)
	if err != nil {
		return err
	}

	transaction := b.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	// Remove all transaction hashes
	for _, txn := range block.Transactions {
		err = transaction.Delete(
			ctx,
			getTransactionHashKey(block.BlockIdentifier.Hash, txn.TransactionIdentifier.Hash),
		)
		if err != nil {
			return err
		}
	}

	// Remove block hash
	err = transaction.Delete(ctx, getBlockHashKey(block.BlockIdentifier.Hash))
	if err != nil {
		return err
	}

	// Remove block
	if err := transaction.Delete(ctx, getBlockKey(block.BlockIdentifier)); err != nil {
		return err
	}

	if err = b.StoreHeadBlockIdentifier(ctx, transaction, block.ParentBlockIdentifier); err != nil {
		return err
	}

	return b.callWorkersAndCommit(ctx, block, transaction, false)
}

func (b *BlockStorage) callWorkersAndCommit(
	ctx context.Context,
	block *types.Block,
	txn DatabaseTransaction,
	adding bool,
) error {
	commitWorkers := make([]CommitWorker, len(b.workers))
	for i, w := range b.workers {
		var cw CommitWorker
		var err error
		if adding {
			cw, err = w.AddingBlock(ctx, block, txn)
		} else {
			cw, err = w.RemovingBlock(ctx, block, txn)
		}
		if err != nil {
			return err
		}

		commitWorkers[i] = cw
	}

	if err := txn.Commit(ctx); err != nil {
		return err
	}

	for _, cw := range commitWorkers {
		if cw == nil {
			continue
		}

		if err := cw(ctx); err != nil {
			return err
		}
	}

	return nil
}

// SetNewStartIndex attempts to remove all blocks
// greater than or equal to the startIndex.
func (b *BlockStorage) SetNewStartIndex(
	ctx context.Context,
	startIndex int64,
) error {
	head, err := b.GetHeadBlockIdentifier(ctx)
	if errors.Is(err, ErrHeadBlockNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if head.Index < startIndex {
		return fmt.Errorf(
			"last processed block %d is less than start index %d",
			head.Index,
			startIndex,
		)
	}

	currBlock := head
	for currBlock.Index >= startIndex {
		log.Printf("Removing block %+v\n", currBlock)
		block, err := b.GetBlock(ctx, currBlock)
		if err != nil {
			return err
		}

		if err := b.RemoveBlock(ctx, block.BlockIdentifier); err != nil {
			return err
		}

		currBlock = block.ParentBlockIdentifier
	}

	return nil
}

// CreateBlockCache populates a slice of blocks with the most recent
// ones in storage.
func (b *BlockStorage) CreateBlockCache(ctx context.Context) []*types.BlockIdentifier {
	cache := []*types.BlockIdentifier{}
	head, err := b.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return cache
	}

	for len(cache) < syncer.PastBlockSize {
		block, err := b.GetBlock(ctx, head)
		if err != nil {
			return cache
		}

		log.Printf("Added %+v to cache\n", block.BlockIdentifier)

		cache = append([]*types.BlockIdentifier{block.BlockIdentifier}, cache...)
		head = block.ParentBlockIdentifier
	}

	return cache
}

// storeHash stores either a block or transaction hash.
// TODO: store block hash in transaction hash value
func (b *BlockStorage) storeHash(
	ctx context.Context,
	transaction DatabaseTransaction,
	hashKey []byte,
) error {
	exists, _, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	if exists {
		return ErrDuplicateKey
	}

	return transaction.Set(ctx, hashKey, []byte(""))
}

// TODO: Wait until a transaction has a depth of X
