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

	"github.com/slowboat0/rosetta-cli/pkg/constructor"
	"github.com/slowboat0/rosetta-cli/pkg/storage"
	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ constructor.Helper = (*ConstructorHelper)(nil)

// ConstructorHelper implements the constructor.Helper
// interface.
type ConstructorHelper struct {
	offlineFetcher *fetcher.Fetcher
	onlineFetcher  *fetcher.Fetcher

	keyStorage       *storage.KeyStorage
	balanceStorage   *storage.BalanceStorage
	coinStorage      *storage.CoinStorage
	broadcastStorage *storage.BroadcastStorage
}

// NewConstructorHelper returns a new *ConstructorHelper.
func NewConstructorHelper(
	offlineFetcher *fetcher.Fetcher,
	onlineFetcher *fetcher.Fetcher,
	keyStorage *storage.KeyStorage,
	balanceStorage *storage.BalanceStorage,
	coinStorage *storage.CoinStorage,
	broadcastStorage *storage.BroadcastStorage,
) *ConstructorHelper {
	return &ConstructorHelper{
		offlineFetcher:   offlineFetcher,
		onlineFetcher:    onlineFetcher,
		keyStorage:       keyStorage,
		balanceStorage:   balanceStorage,
		coinStorage:      coinStorage,
		broadcastStorage: broadcastStorage,
	}
}

// Derive returns a new address for a provided publicKey.
func (c *ConstructorHelper) Derive(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	publicKey *types.PublicKey,
	metadata map[string]interface{},
) (string, map[string]interface{}, error) {
	return c.offlineFetcher.ConstructionDerive(ctx, networkIdentifier, publicKey, metadata)
}

// Preprocess calls the /construction/preprocess endpoint
// on an offline node.
func (c *ConstructorHelper) Preprocess(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	metadata map[string]interface{},
) (map[string]interface{}, error) {
	return c.offlineFetcher.ConstructionPreprocess(
		ctx,
		networkIdentifier,
		intent,
		metadata,
	)
}

// Metadata calls the /construction/metadata endpoint
// using the online node.
func (c *ConstructorHelper) Metadata(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	metadataRequest map[string]interface{},
) (map[string]interface{}, error) {
	return c.onlineFetcher.ConstructionMetadata(
		ctx,
		networkIdentifier,
		metadataRequest,
	)
}

// Payloads calls the /construction/payloads endpoint
// using the offline node.
func (c *ConstructorHelper) Payloads(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	intent []*types.Operation,
	requiredMetadata map[string]interface{},
) (string, []*types.SigningPayload, error) {
	return c.offlineFetcher.ConstructionPayloads(
		ctx,
		networkIdentifier,
		intent,
		requiredMetadata,
	)
}

// Parse calls the /construction/parse endpoint
// using the offline node.
func (c *ConstructorHelper) Parse(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	signed bool,
	transaction string,
) ([]*types.Operation, []string, map[string]interface{}, error) {
	return c.offlineFetcher.ConstructionParse(
		ctx,
		networkIdentifier,
		signed,
		transaction,
	)
}

// Combine calls the /construction/combine endpoint
// using the offline node.
func (c *ConstructorHelper) Combine(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	unsignedTransaction string,
	signatures []*types.Signature,
) (string, error) {
	return c.offlineFetcher.ConstructionCombine(
		ctx,
		networkIdentifier,
		unsignedTransaction,
		signatures,
	)
}

// Hash calls the /construction/hash endpoint
// using the offline node.
func (c *ConstructorHelper) Hash(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	networkTransaction string,
) (*types.TransactionIdentifier, error) {
	return c.offlineFetcher.ConstructionHash(
		ctx,
		networkIdentifier,
		networkTransaction,
	)
}

// Sign invokes the KeyStorage backend
// to sign some payloads.
func (c *ConstructorHelper) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	return c.keyStorage.Sign(ctx, payloads)
}

// StoreKey stores a KeyPair and address
// in KeyStorage.
func (c *ConstructorHelper) StoreKey(
	ctx context.Context,
	address string,
	keyPair *keys.KeyPair,
) error {
	return c.keyStorage.Store(ctx, address, keyPair)
}

// AccountBalance returns the balance
// for a provided address using BalanceStorage.
// If the address balance does not exist,
// 0 will be returned.
func (c *ConstructorHelper) AccountBalance(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*big.Int, error) {
	amount, _, err := c.balanceStorage.GetBalance(
		ctx,
		accountIdentifier,
		currency,
		nil,
	)

	val, ok := new(big.Int).SetString(amount.Value, 10)
	if !ok {
		return nil, fmt.Errorf(
			"could not parse amount for %s",
			accountIdentifier.Address,
		)
	}

	return val, err
}

// CoinBalance returns the balance of the largest
// Coin owned by an address.
func (c *ConstructorHelper) CoinBalance(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*big.Int, *types.CoinIdentifier, error) {
	return c.coinStorage.GetLargestCoin(ctx, accountIdentifier, currency)
}

// LockedAddresses returns a slice of all addresses currently sending or receiving
// funds.
func (c *ConstructorHelper) LockedAddresses(ctx context.Context) ([]string, error) {
	return c.broadcastStorage.LockedAddresses(ctx)
}

// AllBroadcasts returns a slice of all in-progress broadcasts in BroadcastStorage.
func (c *ConstructorHelper) AllBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.GetAllBroadcasts(ctx)
}

// ClearBroadcasts deletes all pending broadcasts.
func (c *ConstructorHelper) ClearBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.ClearBroadcasts(ctx)
}

// Broadcast enqueues a particular intent for broadcast.
func (c *ConstructorHelper) Broadcast(
	ctx context.Context,
	sender string,
	intent []*types.Operation,
	transactionIdentifier *types.TransactionIdentifier,
	payload string,
) error {
	return c.broadcastStorage.Broadcast(
		ctx,
		sender,
		intent,
		transactionIdentifier,
		payload,
	)
}

// AllAddresses returns a slice of all known addresses.
func (c *ConstructorHelper) AllAddresses(ctx context.Context) ([]string, error) {
	return c.keyStorage.GetAllAddresses(ctx)
}

// RandomAmount returns some integer between min and max.
func (c *ConstructorHelper) RandomAmount(min *big.Int, max *big.Int) *big.Int {
	return utils.RandomNumber(min, max)
}
