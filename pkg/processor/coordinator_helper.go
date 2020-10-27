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
	"errors"
	"fmt"
	"log"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	request  = "REQUEST"
	response = "RESPONSE"
	reqerror = "ERROR"
	queue    = "QUEUE"

	constructionDerive     = "/construction/derive"
	constructionPreprocess = "/construction/preprocess"
	constructionMetadata   = "/construction/metadata"
	constructionPayloads   = "/construction/payloads"
	constructionParse      = "/construction/parse"
	constructionCombine    = "/construction/combine"
	constructionHash       = "/construction/hash"
	constructionSubmit     = "/construction/submit"

	argNetwork               = "network_identifier"
	argMetadata              = "metadata"
	argError                 = "error"
	argAccount               = "account_identifier"
	argIntent                = "intent"
	argPublicKeys            = "public_keys"
	argUnsignedTransaction   = "unsigned_transaction"
	argTransactionIdentifier = "transaction_identifier"
	argNetworkTransaction    = "network_transaction"
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

	// quiet determines if requests/responses logging
	// should be silenced.
	quiet bool
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
	quiet bool,
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
		quiet:                quiet,
	}
}

// DatabaseTransaction returns a new write-ready storage.DatabaseTransaction.
func (c *CoordinatorHelper) DatabaseTransaction(ctx context.Context) storage.DatabaseTransaction {
	return c.database.NewDatabaseTransaction(ctx, true)
}

type arg struct {
	name string
	val  interface{}
}

// verboseLog logs a request or response if c.verbose is true.
func (c *CoordinatorHelper) verboseLog(reqres string, endpoint string, args ...arg) {
	if c.quiet {
		return
	}

	l := fmt.Sprintf("%s %s", reqres, endpoint)
	for _, a := range args {
		l = fmt.Sprintf("%s %s:%s", l, a.name, types.PrintStruct(a.val))
	}

	log.Println(l)
}

// Derive returns a new address for a provided publicKey.
func (c *CoordinatorHelper) Derive(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	publicKey *types.PublicKey,
	metadata map[string]interface{},
) (*types.AccountIdentifier, map[string]interface{}, error) {
	c.verboseLog(request, constructionDerive,
		arg{argNetwork, networkIdentifier},
		arg{"public_key", publicKey},
		arg{argMetadata, metadata},
	)
	account, metadata, fetchErr := c.offlineFetcher.ConstructionDerive(
		ctx,
		networkIdentifier,
		publicKey,
		metadata,
	)
	if fetchErr != nil {
		c.verboseLog(reqerror, constructionDerive, arg{argError, fetchErr})
		return nil, nil, fetchErr.Err
	}

	c.verboseLog(response, constructionDerive,
		arg{argAccount, account},
		arg{argMetadata, metadata},
	)
	return account, metadata, nil
}

// Preprocess calls the /construction/preprocess endpoint
// on an offline node.
func (c *CoordinatorHelper) Preprocess(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	metadata map[string]interface{},
) (map[string]interface{}, []*types.AccountIdentifier, error) {
	c.verboseLog(request, constructionPreprocess,
		arg{argNetwork, networkIdentifier},
		arg{argIntent, intent},
		arg{argMetadata, metadata},
	)
	options, requiredPublicKeys, fetchErr := c.offlineFetcher.ConstructionPreprocess(
		ctx,
		networkIdentifier,
		intent,
		metadata,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionPreprocess, arg{argError, fetchErr})
		return nil, nil, fetchErr.Err
	}

	c.verboseLog(response, constructionPreprocess,
		arg{"options", options},
		arg{"required_public_keys", requiredPublicKeys},
	)
	return options, requiredPublicKeys, nil
}

// Metadata calls the /construction/metadata endpoint
// using the online node.
func (c *CoordinatorHelper) Metadata(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	metadataRequest map[string]interface{},
	publicKeys []*types.PublicKey,
) (map[string]interface{}, []*types.Amount, error) {
	c.verboseLog(request, constructionMetadata,
		arg{argNetwork, networkIdentifier},
		arg{argMetadata, metadataRequest},
		arg{argPublicKeys, publicKeys},
	)
	metadata, suggestedFee, fetchErr := c.onlineFetcher.ConstructionMetadata(
		ctx,
		networkIdentifier,
		metadataRequest,
		publicKeys,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionMetadata, arg{argError, fetchErr})
		return nil, nil, fetchErr.Err
	}

	c.verboseLog(response, constructionMetadata,
		arg{argMetadata, metadata},
		arg{"suggested_fee", suggestedFee},
	)
	return metadata, suggestedFee, nil
}

// Payloads calls the /construction/payloads endpoint
// using the offline node.
func (c *CoordinatorHelper) Payloads(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	requiredMetadata map[string]interface{},
	publicKeys []*types.PublicKey,
) (string, []*types.SigningPayload, error) {
	c.verboseLog(request, constructionPayloads,
		arg{argNetwork, networkIdentifier},
		arg{argIntent, intent},
		arg{argPublicKeys, publicKeys},
	)
	res, payloads, fetchErr := c.offlineFetcher.ConstructionPayloads(
		ctx,
		networkIdentifier,
		intent,
		requiredMetadata,
		publicKeys,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionPayloads, arg{argError, fetchErr})
		return "", nil, fetchErr.Err
	}

	c.verboseLog(response, constructionPayloads,
		arg{argUnsignedTransaction, res},
		arg{"payloads", payloads},
	)
	return res, payloads, nil
}

// Parse calls the /construction/parse endpoint
// using the offline node.
func (c *CoordinatorHelper) Parse(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	signed bool,
	transaction string,
) ([]*types.Operation, []*types.AccountIdentifier, map[string]interface{}, error) {
	c.verboseLog(request, constructionParse,
		arg{argNetwork, networkIdentifier},
		arg{"signed", signed},
		arg{"transaction", transaction},
	)
	ops, signers, metadata, fetchErr := c.offlineFetcher.ConstructionParse(
		ctx,
		networkIdentifier,
		signed,
		transaction,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionParse, arg{argError, fetchErr})
		return nil, nil, nil, fetchErr.Err
	}

	c.verboseLog(response, constructionParse,
		arg{"operations", ops},
		arg{"signers", signers},
		arg{argMetadata, metadata},
	)
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
	c.verboseLog(request, constructionCombine,
		arg{argNetwork, networkIdentifier},
		arg{argUnsignedTransaction, unsignedTransaction},
		arg{"signatures", signatures},
	)
	res, fetchErr := c.offlineFetcher.ConstructionCombine(
		ctx,
		networkIdentifier,
		unsignedTransaction,
		signatures,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionCombine, arg{argError, fetchErr})
		return "", fetchErr.Err
	}

	c.verboseLog(response, constructionCombine, arg{argNetworkTransaction, res})
	return res, nil
}

// Hash calls the /construction/hash endpoint
// using the offline node.
func (c *CoordinatorHelper) Hash(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	networkTransaction string,
) (*types.TransactionIdentifier, error) {
	c.verboseLog(request, constructionHash,
		arg{argNetwork, networkIdentifier},
		arg{argNetworkTransaction, networkTransaction},
	)
	res, fetchErr := c.offlineFetcher.ConstructionHash(
		ctx,
		networkIdentifier,
		networkTransaction,
	)

	if fetchErr != nil {
		c.verboseLog(reqerror, constructionHash, arg{argError, fetchErr})
		return nil, fetchErr.Err
	}

	c.verboseLog(response, constructionHash, arg{argTransactionIdentifier, res})
	return res, nil
}

// Sign invokes the KeyStorage backend
// to sign some payloads.
func (c *CoordinatorHelper) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	return c.keyStorage.Sign(ctx, payloads)
}

// GetKey is called to get the *types.KeyPair
// associated with an address.
func (c *CoordinatorHelper) GetKey(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	account *types.AccountIdentifier,
) (*keys.KeyPair, error) {
	return c.keyStorage.GetTransactional(ctx, dbTx, account)
}

// StoreKey stores a KeyPair and address
// in KeyStorage.
func (c *CoordinatorHelper) StoreKey(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	account *types.AccountIdentifier,
	keyPair *keys.KeyPair,
) error {
	// We optimisically add the interesting address although the dbTx could be reverted.
	c.balanceStorageHelper.AddInterestingAddress(account.Address)

	_, _ = c.counterStorage.UpdateTransactional(
		ctx,
		dbTx,
		storage.AddressesCreatedCounter,
		big.NewInt(1),
	)
	return c.keyStorage.StoreTransactional(ctx, account, keyPair, dbTx)
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
	headBlock, err := c.blockStorage.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	if headBlock == nil {
		return nil, errors.New("no blocks synced")
	}

	return c.balanceStorage.GetOrSetBalanceTransactional(
		ctx,
		dbTx,
		accountIdentifier,
		currency,
		headBlock,
	)
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

// LockedAccounts returns a slice of all accounts currently sending or receiving
// funds.
func (c *CoordinatorHelper) LockedAccounts(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) ([]*types.AccountIdentifier, error) {
	return c.broadcastStorage.LockedAccounts(ctx, dbTx)
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
	c.verboseLog(queue, constructionSubmit,
		arg{argNetwork, network},
		arg{argIntent, intent},
		arg{argTransactionIdentifier, transactionIdentifier},
		arg{argNetworkTransaction, payload},
	)
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

// AllAccounts returns a slice of all known accounts.
func (c *CoordinatorHelper) AllAccounts(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) ([]*types.AccountIdentifier, error) {
	return c.keyStorage.GetAllAccountsTransactional(ctx, dbTx)
}

// HeadBlockExists returns a boolean indicating if a block has been
// synced by BlockStorage.
func (c *CoordinatorHelper) HeadBlockExists(ctx context.Context) bool {
	headBlock, _ := c.blockStorage.GetHeadBlockIdentifier(ctx)

	return headBlock != nil
}
