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
	"fmt"
	"log"
	"sync"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ BlockWorker = (*BroadcastStorage)(nil)

const (
	transactionBroadcastNamespace = "transaction-broadcast"

	// depthOffset is used for adjusting depth checks because
	// depth is "indexed by 1". Meaning, if a transaction is in
	// tip it has depth 1.
	depthOffset = 1
)

func getBroadcastKey(transactionIdentifier *types.TransactionIdentifier) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", transactionBroadcastNamespace, transactionIdentifier.Hash),
	)
}

// BroadcastStorage implements storage methods for managing
// transaction broadcast.
type BroadcastStorage struct {
	db      Database
	helper  BroadcastStorageHelper
	handler BroadcastStorageHandler

	confirmationDepth   int64
	staleDepth          int64
	broadcastLimit      int
	tipDelay            int64
	broadcastBehindTip  bool
	blockBroadcastLimit int

	// Running BroadcastAll concurrently
	// could cause corruption.
	broadcastAllMutex sync.Mutex
}

// BroadcastStorageHelper is used by BroadcastStorage to submit transactions
// and find said transaction in blocks on-chain.
type BroadcastStorageHelper interface {
	// CurrentBlockIdentifier is called before transaction broadcast and is used
	// to determine if a transaction broadcast is stale.
	CurrentBlockIdentifier(
		context.Context,
	) (*types.BlockIdentifier, error) // used to determine if should rebroadcast

	// AtTip is called before transaction broadcast to determine if we are at tip.
	AtTip(
		context.Context,
		int64,
	) (bool, error)

	// FindTransaction looks for the provided TransactionIdentifier in processed
	// blocks and returns the block identifier containing the most recent sighting
	// and the transaction seen in that block.
	FindTransaction(
		context.Context,
		*types.TransactionIdentifier,
		DatabaseTransaction,
	) (*types.BlockIdentifier, *types.Transaction, error) // used to confirm

	// BroadcastTransaction broadcasts a transaction to a Rosetta implementation
	// and returns the *types.TransactionIdentifier returned by the implementation.
	BroadcastTransaction(
		context.Context,
		string,
	) (*types.TransactionIdentifier, error) // handle initial broadcast + confirm matches provided + rebroadcast if stale
}

// BroadcastStorageHandler is invoked when a transaction is confirmed on-chain
// or when a transaction is considered stale.
type BroadcastStorageHandler interface {
	// TransactionConfirmed is called when a transaction is observed on-chain for the
	// last time at a block height < current block height - confirmationDepth.
	TransactionConfirmed(
		context.Context,
		*types.BlockIdentifier,
		*types.Transaction,
		[]*types.Operation,
	) error // can use locked account again + confirm matches intent + update logger

	// TransactionStale is called when a transaction has not yet been
	// seen on-chain and is considered stale. This occurs when
	// current block height - last broadcast > staleDepth.
	TransactionStale(
		context.Context,
		*types.TransactionIdentifier,
	) error // log in counter (rebroadcast should occur here)

	// BroadcastFailed is called when another transaction broadcast would
	// put it over the provided broadcast limit.
	BroadcastFailed(
		context.Context,
		*types.TransactionIdentifier,
		[]*types.Operation,
	) error
}

// Broadcast is persisted to the db to track transaction broadcast.
type Broadcast struct {
	Identifier    *types.TransactionIdentifier `json:"identifier"`
	Sender        string                       `json:"sender"`
	Intent        []*types.Operation           `json:"intent"`
	Payload       string                       `json:"payload"`
	LastBroadcast *types.BlockIdentifier       `json:"broadcast_at"`
	Broadcasts    int                          `json:"broadcasts"`
}

// NewBroadcastStorage returns a new BroadcastStorage.
func NewBroadcastStorage(
	db Database,
	confirmationDepth int64,
	staleDepth int64,
	broadcastLimit int,
	tipDelay int64,
	broadcastBehindTip bool,
	blockBroadcastLimit int,
) *BroadcastStorage {
	return &BroadcastStorage{
		db:                  db,
		confirmationDepth:   confirmationDepth,
		staleDepth:          staleDepth,
		broadcastLimit:      broadcastLimit,
		tipDelay:            tipDelay,
		broadcastBehindTip:  broadcastBehindTip,
		blockBroadcastLimit: blockBroadcastLimit,
	}
}

// Initialize adds a BroadcastStorageHelper and BroadcastStorageHandler to BroadcastStorage.
// This must be called prior to syncing!
func (b *BroadcastStorage) Initialize(
	helper BroadcastStorageHelper,
	handler BroadcastStorageHandler,
) {
	b.helper = helper
	b.handler = handler
}

func (b *BroadcastStorage) addBlockCommitWorker(
	ctx context.Context,
	staleTransactions []*types.TransactionIdentifier,
	confirmedTransactions []*Broadcast,
	foundTransactions []*types.Transaction,
	foundBlocks []*types.BlockIdentifier,
) error {
	for _, stale := range staleTransactions {
		if err := b.handler.TransactionStale(ctx, stale); err != nil {
			return fmt.Errorf("%w: unable to handle stale transaction %s", err, stale.Hash)
		}
	}

	for i, broadcast := range confirmedTransactions {
		err := b.handler.TransactionConfirmed(
			ctx,
			foundBlocks[i],
			foundTransactions[i],
			broadcast.Intent,
		)
		if err != nil {
			return fmt.Errorf(
				"%w: unable to handle confirmed transaction %s",
				err,
				broadcast.Identifier.Hash,
			)
		}
	}

	if err := b.BroadcastAll(ctx, true); err != nil {
		return fmt.Errorf("%w: unable to broadcast pending transactions", err)
	}

	return nil
}

// AddingBlock is called by BlockStorage when adding a block.
func (b *BroadcastStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	staleTransactions := []*types.TransactionIdentifier{}
	confirmedTransactions := []*Broadcast{}
	foundTransactions := []*types.Transaction{}
	foundBlocks := []*types.BlockIdentifier{}

	for _, broadcast := range broadcasts {
		if broadcast.LastBroadcast == nil {
			continue
		}

		key := getBroadcastKey(broadcast.Identifier)

		// We perform the FindTransaction search in the context of the block database
		// transaction so we can access any transactions of depth 1 (in the current
		// block).
		foundBlock, foundTransaction, err := b.helper.FindTransaction(ctx, broadcast.Identifier, transaction)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to determine if transaction was seen", err)
		}

		// Check if we should mark the broadcast as stale
		if foundBlock == nil &&
			block.BlockIdentifier.Index-broadcast.LastBroadcast.Index >= b.staleDepth-depthOffset {
			staleTransactions = append(staleTransactions, broadcast.Identifier)
			broadcast.LastBroadcast = nil
			bytes, err := encode(broadcast)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to encode updated broadcast", err)
			}

			if err := transaction.Set(ctx, key, bytes); err != nil {
				return nil, fmt.Errorf("%w: unable to update broadcast", err)
			}

			continue
		}

		// Continue if we are still waiting for a broadcast to appear and it isn't stale
		if foundBlock == nil {
			continue
		}

		// Check if we should mark the transaction as confirmed
		if block.BlockIdentifier.Index-foundBlock.Index >= b.confirmationDepth-depthOffset {
			confirmedTransactions = append(confirmedTransactions, broadcast)
			foundTransactions = append(foundTransactions, foundTransaction)
			foundBlocks = append(foundBlocks, foundBlock)

			if err := transaction.Delete(ctx, key); err != nil {
				return nil, fmt.Errorf("%w: unable to delete confirmed broadcast", err)
			}
		}
	}

	return func(ctx context.Context) error {
		return b.addBlockCommitWorker(
			ctx,
			staleTransactions,
			confirmedTransactions,
			foundTransactions,
			foundBlocks,
		)
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
// TODO: error if transaction removed after confirmed (means confirmation depth not deep enough)
func (b *BroadcastStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	return nil, nil
}

// Broadcast is called when a caller wants a transaction to be broadcast and tracked.
// The caller SHOULD NOT broadcast the transaction before calling this function.
func (b *BroadcastStorage) Broadcast(
	ctx context.Context,
	sender string,
	intent []*types.Operation,
	transactionIdentifier *types.TransactionIdentifier,
	payload string,
) error {
	txn := b.db.NewDatabaseTransaction(ctx, true)
	defer txn.Discard(ctx)

	broadcastKey := getBroadcastKey(transactionIdentifier)

	exists, _, err := txn.Get(ctx, broadcastKey)
	if err != nil {
		return fmt.Errorf("%w: unable to determine if already broadcasting transaction", err)
	}

	if exists {
		return fmt.Errorf("already broadcasting transaction %s", transactionIdentifier.Hash)
	}

	bytes, err := encode(&Broadcast{
		Identifier: transactionIdentifier,
		Sender:     sender,
		Intent:     intent,
		Payload:    payload,
		Broadcasts: 0,
	})
	if err != nil {
		return fmt.Errorf("%w: unable to encode broadcast", err)
	}

	if err := txn.Set(ctx, broadcastKey, bytes); err != nil {
		return fmt.Errorf("%w: unable to set broadcast", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit broadcast", err)
	}

	// Broadcast all pending transactions (instead of waiting
	// until after processing the next block).
	if err := b.BroadcastAll(ctx, true); err != nil {
		return fmt.Errorf("%w: unable to broadcast pending transactions", err)
	}

	return nil
}

// GetAllBroadcasts returns all currently in-process broadcasts.
func (b *BroadcastStorage) GetAllBroadcasts(ctx context.Context) ([]*Broadcast, error) {
	rawBroadcasts, err := b.db.Scan(ctx, []byte(transactionBroadcastNamespace))
	if err != nil {
		return nil, fmt.Errorf("%w: unable to scan for all broadcasts", err)
	}

	broadcasts := make([]*Broadcast, len(rawBroadcasts))
	for i, rawBroadcast := range rawBroadcasts {
		var b Broadcast
		if err := decode(rawBroadcast, &b); err != nil {
			return nil, fmt.Errorf("%w: unable to decode broadcast", err)
		}

		broadcasts[i] = &b
	}

	return broadcasts, nil
}

func (b *BroadcastStorage) performBroadcast(
	ctx context.Context,
	broadcast *Broadcast,
	onlyEligible bool,
) error {
	bytes, err := encode(broadcast)
	if err != nil {
		return fmt.Errorf("%w: unable to encode broadcast", err)
	}

	txn := b.db.NewDatabaseTransaction(ctx, true)
	defer txn.Discard(ctx)

	if err := txn.Set(ctx, getBroadcastKey(broadcast.Identifier), bytes); err != nil {
		return fmt.Errorf("%w: unable to update broadcast", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit broadcast update", err)
	}

	if !onlyEligible {
		log.Printf("Broadcasting: %s\n", types.PrettyPrintStruct(broadcast))
	}

	broadcastIdentifier, err := b.helper.BroadcastTransaction(ctx, broadcast.Payload)
	if err != nil {
		// Don't error on broadcast failure, retries will automatically be handled.
		log.Printf(
			"%s: unable to broadcast transaction %s",
			err.Error(),
			broadcast.Identifier.Hash,
		)

		return nil
	}

	if types.Hash(broadcastIdentifier) != types.Hash(broadcast.Identifier) {
		return fmt.Errorf(
			"transaction hash returned by broadcast %s does not match expected %s",
			broadcastIdentifier.Hash,
			broadcast.Identifier.Hash,
		)
	}

	return nil
}

// BroadcastAll broadcasts all transactions in BroadcastStorage. If onlyEligible
// is set to true, then only transactions that should be broadcast again
// are actually broadcast.
func (b *BroadcastStorage) BroadcastAll(ctx context.Context, onlyEligible bool) error {
	// Corruption can occur if we run this concurrently.
	b.broadcastAllMutex.Lock()
	defer b.broadcastAllMutex.Unlock()

	currBlock, err := b.helper.CurrentBlockIdentifier(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to get current block identifier", err)
	}

	// We have not yet synced a block and should wait to broadcast
	// until we do so (otherwise we can't track last broadcast correctly).
	if currBlock == nil {
		return nil
	}

	// Wait to broadcast transaction until close to tip
	atTip, err := b.helper.AtTip(ctx, b.tipDelay)
	if err != nil {
		return fmt.Errorf("%w: unable to determine if at tip", err)
	}

	if (!atTip && !b.broadcastBehindTip) && onlyEligible {
		return nil
	}

	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	attemptedBroadcasts := 0
	for _, broadcast := range broadcasts {
		// When a transaction should be broadcast, its last broadcast field must
		// be set to nil.
		if broadcast.LastBroadcast != nil && onlyEligible {
			continue
		}

		if broadcast.Broadcasts >= b.broadcastLimit {
			txn := b.db.NewDatabaseTransaction(ctx, true)
			defer txn.Discard(ctx)

			if err := txn.Delete(ctx, getBroadcastKey(broadcast.Identifier)); err != nil {
				return fmt.Errorf("%w: unable to delete broadcast", err)
			}

			if err := txn.Commit(ctx); err != nil {
				return fmt.Errorf("%w: unable to commit broadcast delete", err)
			}

			if err := b.handler.BroadcastFailed(ctx, broadcast.Identifier, broadcast.Intent); err != nil {
				return fmt.Errorf("%w: unable to handle broadcast failure", err)
			}

			continue
		}

		// Limit the number of transactions we attempt to broadcast
		// at a given block.
		if attemptedBroadcasts >= b.blockBroadcastLimit {
			continue
		}
		attemptedBroadcasts++

		// We set the last broadcast value before broadcast so we don't accidentally
		// re-broadcast if exiting between broadcasting the transaction and updating
		// the value in the database. If the transaction is never really broadcast,
		// it will be rebroadcast when it is considered stale!
		broadcast.LastBroadcast = currBlock
		broadcast.Broadcasts++

		if err := b.performBroadcast(ctx, broadcast, onlyEligible); err != nil {
			return fmt.Errorf("%w: unable to perform broadcast", err)
		}
	}

	return nil
}

// LockedAddresses returns all addresses currently active in transaction broadcasts.
// The caller SHOULD NOT broadcast a transaction from an account if it is
// considered locked!
func (b *BroadcastStorage) LockedAddresses(ctx context.Context) ([]string, error) {
	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	addressMap := map[string]struct{}{}
	for _, broadcast := range broadcasts {
		for _, op := range broadcast.Intent {
			if op.Account == nil {
				continue
			}

			addressMap[op.Account.Address] = struct{}{}
		}
	}

	addresses := []string{}
	for k := range addressMap {
		addresses = append(addresses, k)
	}

	return addresses, nil
}

// ClearBroadcasts deletes all in-progress broadcasts from BroadcastStorage. This
// is useful when there is some construction error and all pending broadcasts
// will fail and should be cleared instead of re-attempting.
func (b *BroadcastStorage) ClearBroadcasts(ctx context.Context) ([]*Broadcast, error) {
	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	txn := b.db.NewDatabaseTransaction(ctx, true)
	for _, broadcast := range broadcasts {
		if err := txn.Delete(ctx, getBroadcastKey(broadcast.Identifier)); err != nil {
			return nil, fmt.Errorf(
				"%w: unable to delete broadcast %s",
				err,
				broadcast.Identifier.Hash,
			)
		}
	}

	if err := txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: unable to delete broadcasts", err)
	}

	return broadcasts, nil
}
