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

package processor

import (
	"context"
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ coordinator.Helper = (*CoordinatorHelper)(nil)

// CoordinatorHelper implements the Coordinator.Helper
// interface.
type CoordinatorHelper struct {
	offlineFetcher *fetcher.Fetcher
	onlineFetcher  *fetcher.Fetcher

	database         storage.Database
	blockStorage     *storage.BlockStorage
	keyStorage       *storage.KeyStorage
	balanceStorage   *storage.BalanceStorage
	coinStorage      *storage.CoinStorage
	broadcastStorage *storage.BroadcastStorage
	counterStorage   *storage.CounterStorage

	balanceStorageHelper *BalanceStorageHelper
}

// NewCoordinatorHelper returns a new *CoordinatorHelper.
func NewCoordinatorHelper(
	offlineFetcher *fetcher.Fetcher,
	onlineFetcher *fetcher.Fetcher,
	database storage.Database,
	blockStorage *storage.BlockStorage,
	keyStorage *storage.KeyStorage,
	balanceStorage *storage.BalanceStorage,
	coinStorage *storage.CoinStorage,
	broadcastStorage *storage.BroadcastStorage,
	balanceStorageHelper *BalanceStorageHelper,
	counterStorage *storage.CounterStorage,
) *CoordinatorHelper {
	return &CoordinatorHelper{
		offlineFetcher:       offlineFetcher,
		onlineFetcher:        onlineFetcher,
		database:             database,
		blockStorage:         blockStorage,
		keyStorage:           keyStorage,
		balanceStorage:       balanceStorage,
		coinStorage:          coinStorage,
		broadcastStorage:     broadcastStorage,
		counterStorage:       counterStorage,
		balanceStorageHelper: balanceStorageHelper,
	}
}

// DatabaseTransaction returns a new write-ready storage.DatabaseTransaction.
func (c *CoordinatorHelper) DatabaseTransaction(ctx context.Context) storage.DatabaseTransaction {
	return c.database.NewDatabaseTransaction(ctx, true)
}

// Derive returns a new address for a provided publicKey.
func (c *CoordinatorHelper) Derive(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	publicKey *types.PublicKey,
	metadata map[string]interface{},
) (string, map[string]interface{}, error) {
	add, metadata, fetchErr := c.offlineFetcher.ConstructionDerive(
		ctx,
		networkIdentifier,
		publicKey,
		metadata,
	)
	if fetchErr != nil {
		return "", nil, fetchErr.Err
	}

	return add, metadata, nil
}

// Preprocess calls the /construction/preprocess endpoint
// on an offline node.
func (c *CoordinatorHelper) Preprocess(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	metadata map[string]interface{},
) (map[string]interface{}, error) {
	res, fetchErr := c.offlineFetcher.ConstructionPreprocess(
		ctx,
		networkIdentifier,
		intent,
		metadata,
	)

	if fetchErr != nil {
		return nil, fetchErr.Err
	}

	return res, nil
}

// Metadata calls the /construction/metadata endpoint
// using the online node.
func (c *CoordinatorHelper) Metadata(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	metadataRequest map[string]interface{},
) (map[string]interface{}, error) {
	res, fetchErr := c.offlineFetcher.ConstructionMetadata(
		ctx,
		networkIdentifier,
		metadataRequest,
	)

	if fetchErr != nil {
		return nil, fetchErr.Err
	}

	return res, nil
}

// Payloads calls the /construction/payloads endpoint
// using the offline node.
func (c *CoordinatorHelper) Payloads(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	requiredMetadata map[string]interface{},
) (string, []*types.SigningPayload, error) {
	res, payloads, fetchErr := c.offlineFetcher.ConstructionPayloads(
		ctx,
		networkIdentifier,
		intent,
		requiredMetadata,
	)

	if fetchErr != nil {
		return "", nil, fetchErr.Err
	}

	return res, payloads, nil
}

// Parse calls the /construction/parse endpoint
// using the offline node.
func (c *CoordinatorHelper) Parse(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	signed bool,
	transaction string,
) ([]*types.Operation, []string, map[string]interface{}, error) {
	ops, signers, metadata, fetchErr := c.offlineFetcher.ConstructionParse(
		ctx,
		networkIdentifier,
		signed,
		transaction,
	)

	if fetchErr != nil {
		return nil, nil, nil, fetchErr.Err
	}

	return ops, signers, metadata, nil
}

// Combine calls the /construction/combine endpoint
// using the offline node.
func (c *CoordinatorHelper) Combine(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	unsignedTransaction string,
	signatures []*types.Signature,
) (string, error) {
	res, fetchErr := c.offlineFetcher.ConstructionCombine(
		ctx,
		networkIdentifier,
		unsignedTransaction,
		signatures,
	)

	if fetchErr != nil {
		return "", fetchErr.Err
	}

	return res, nil
}

// Hash calls the /construction/hash endpoint
// using the offline node.
func (c *CoordinatorHelper) Hash(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	networkTransaction string,
) (*types.TransactionIdentifier, error) {
	res, fetchErr := c.offlineFetcher.ConstructionHash(
		ctx,
		networkIdentifier,
		networkTransaction,
	)

	if fetchErr != nil {
		return nil, fetchErr.Err
	}

	return res, nil
}

// Sign invokes the KeyStorage backend
// to sign some payloads.
func (c *CoordinatorHelper) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	fmt.Printf("signing payloads %s\n", types.PrintStruct(payloads))
	return c.keyStorage.Sign(ctx, payloads)
}

// StoreKey stores a KeyPair and address
// in KeyStorage.
func (c *CoordinatorHelper) StoreKey(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	address string,
	keyPair *keys.KeyPair,
) error {
	// We optimisically add the interesting address although the dbTx could be reverted.
	c.balanceStorageHelper.AddInterestingAddress(address)

	_, _ = c.counterStorage.UpdateTransactional(ctx, dbTx, storage.AddressesCreatedCounter, big.NewInt(1))
	return c.keyStorage.StoreTransactional(ctx, address, keyPair, dbTx)
}

// Balance returns the balance
// for a provided address using BalanceStorage.
// If the address balance does not exist,
// 0 will be returned.
func (c *CoordinatorHelper) Balance(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*types.Amount, error) {
	amount, _, err := c.balanceStorage.GetBalanceTransactional(
		ctx,
		dbTx,
		accountIdentifier,
		currency,
		nil,
	)

	return amount, err
}

// Coins returns all *types.Coin owned by
// an account.
func (c *CoordinatorHelper) Coins(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) ([]*types.Coin, error) {
	coins, _, err := c.coinStorage.GetCoinsTransactional(
		ctx,
		dbTx,
		accountIdentifier,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get coins", err)
	}

	coinsToReturn := []*types.Coin{}
	for _, coin := range coins {
		if types.Hash(coin.Amount.Currency) != types.Hash(currency) {
			continue
		}

		coinsToReturn = append(coinsToReturn, coin)
	}

	return coinsToReturn, nil
}

// LockedAddresses returns a slice of all addresses currently sending or receiving
// funds.
func (c *CoordinatorHelper) LockedAddresses(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) ([]string, error) {
	return c.broadcastStorage.LockedAddresses(ctx, dbTx)
}

// AllBroadcasts returns a slice of all in-progress broadcasts in BroadcastStorage.
func (c *CoordinatorHelper) AllBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.GetAllBroadcasts(ctx)
}

// ClearBroadcasts deletes all pending broadcasts.
func (c *CoordinatorHelper) ClearBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.ClearBroadcasts(ctx)
}

// Broadcast enqueues a particular intent for broadcast.
func (c *CoordinatorHelper) Broadcast(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	identifier string,
	network *types.NetworkIdentifier,
	intent []*types.Operation,
	transactionIdentifier *types.TransactionIdentifier,
	payload string,
	confirmationDepth int64,
) error {
	return c.broadcastStorage.Broadcast(
		ctx,
		dbTx,
		identifier,
		network,
		intent,
		transactionIdentifier,
		payload,
		confirmationDepth,
	)
}

// BroadcastAll attempts to broadcast all ready transactions.
func (c *CoordinatorHelper) BroadcastAll(
	ctx context.Context,
) error {
	return c.broadcastStorage.BroadcastAll(ctx, true)
}

// AllAddresses returns a slice of all known addresses.
func (c *CoordinatorHelper) AllAddresses(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) ([]string, error) {
	return c.keyStorage.GetAllAddressesTransactional(ctx, dbTx)
}

// HeadBlockExists returns a boolean indicating if a block has been
// synced by BlockStorage.
func (c *CoordinatorHelper) HeadBlockExists(ctx context.Context) bool {
	headBlock, _ := c.blockStorage.GetHeadBlockIdentifier(ctx)

	return headBlock != nil
}
