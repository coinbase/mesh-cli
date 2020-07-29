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

	"github.com/coinbase/rosetta-cli/internal/constructor"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ constructor.Helper = (*ConstructorHelper)(nil)

type ConstructorHelper struct {
	offlineFetcher *fetcher.Fetcher
	onlineFetcher  *fetcher.Fetcher

	keyStorage       *storage.KeyStorage
	balanceStorage   *storage.BalanceStorage
	coinStorage      *storage.CoinStorage
	broadcastStorage *storage.BroadcastStorage
}

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

func (c *ConstructorHelper) Derive(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	publicKey *types.PublicKey,
	metadata map[string]interface{},
) (string, map[string]interface{}, error) {
	return c.offlineFetcher.ConstructionDerive(ctx, networkIdentifier, publicKey, metadata)
}

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

func (c *ConstructorHelper) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	return c.keyStorage.Sign(ctx, payloads)
}

func (c *ConstructorHelper) StoreKey(
	ctx context.Context,
	address string,
	keyPair *keys.KeyPair,
) error {
	return c.keyStorage.Store(ctx, address, keyPair)
}

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

func (c *ConstructorHelper) CoinBalance(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*big.Int, *types.CoinIdentifier, error) {
	// For UTXO-based chains, return the largest UTXO as the spendable balance.
	coins, err := c.coinStorage.GetCoins(ctx, accountIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to get utxo balance for %s",
			err,
			accountIdentifier.Address,
		)
	}

	bal := big.NewInt(0)
	var coinIdentifier *types.CoinIdentifier
	for _, coin := range coins {
		if types.Hash(
			coin.Operation.Amount.Currency,
		) != types.Hash(
			currency,
		) {
			continue
		}

		val, ok := new(big.Int).SetString(coin.Operation.Amount.Value, 10)
		if !ok {
			return nil, nil, fmt.Errorf(
				"could not parse amount for coin %s",
				coin.Identifier.Identifier,
			)
		}

		if bal.Cmp(val) == -1 {
			bal = val
			coinIdentifier = coin.Identifier
		}
	}

	return bal, coinIdentifier, nil
}

func (c *ConstructorHelper) LockedAddresses(ctx context.Context) ([]string, error) {
	return c.broadcastStorage.LockedAddresses(ctx)
}

func (c *ConstructorHelper) AllBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.GetAllBroadcasts(ctx)
}

func (c *ConstructorHelper) ClearBroadcasts(ctx context.Context) ([]*storage.Broadcast, error) {
	return c.broadcastStorage.ClearBroadcasts(ctx)
}

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

func (c *ConstructorHelper) AllAddresses(ctx context.Context) ([]string, error) {
	return c.keyStorage.GetAllAddresses(ctx)
}
