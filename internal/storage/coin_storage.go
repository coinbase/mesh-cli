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

	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	coinNamespace        = "coinNamespace"
	coinAccountNamespace = "coinAccountNamespace"

	coinCreated = "utxo_created"
	coinSpent   = "utxo_spent"
)

var _ BlockWorker = (*CoinStorage)(nil)

// CoinStorage implements storage methods for storing
// UTXOs.
type CoinStorage struct {
	db Database
}

// NewCoinStorage returns a new CoinStorage.
func NewCoinStorage(
	db Database,
) *CoinStorage {
	return &CoinStorage{
		db: db,
	}
}

type Coin struct {
	Identifier  string             `json:"identifier"` // uses "utxo_created" or "utxo_spent"
	Transaction *types.Transaction `json:"transaction"`
	Operation   *types.Operation   `json:"operation"`
}

func getCoinKey(identifier string) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinNamespace, identifier))
}

func getCoinAccountKey(accountIdentifier *types.AccountIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinAccountNamespace, types.Hash(accountIdentifier)))
}

func (c *CoinStorage) tryAddingCoin(ctx context.Context, transaction DatabaseTransaction, blockTransaction *types.Transaction, operation *types.Operation, identiferKey string) error {
	rawIdentifier, ok := operation.Metadata[identiferKey]
	if ok {
		coinIdentifier, ok := rawIdentifier.(string)
		if !ok {
			return fmt.Errorf("unable to parse created coin %v", rawIdentifier)
		}

		newCoin := &Coin{
			Identifier:  coinIdentifier,
			Transaction: blockTransaction,
			Operation:   operation,
		}

		encodedResult, err := encode(newCoin)
		if err != nil {
			return fmt.Errorf("%w: unable to encode coin data", err)
		}

		if err := transaction.Set(ctx, getCoinKey(coinIdentifier), encodedResult); err != nil {
			return fmt.Errorf("%w: unable to store coin", err)
		}

		accountExists, val, err := transaction.Get(ctx, getCoinAccountKey(operation.Account))
		if err != nil {
			return fmt.Errorf("%w: unable to query coin account", err)
		}

		var coins map[string]struct{}
		if !accountExists {
			coins = map[string]struct{}{}
			coins[coinIdentifier] = struct{}{}
		} else {
			if err := decode(val, &coins); err != nil {
				return fmt.Errorf("%w: unable to decode coin account", err)
			}

			if _, exists := coins[coinIdentifier]; exists {
				return fmt.Errorf("coin %s already exists in account %s", coinIdentifier, types.PrettyPrintStruct(operation.Account))
			}
		}

		encodedResult, err = encode(coins)
		if err != nil {
			return fmt.Errorf("%w: unable to encode coins", err)
		}

		if err := transaction.Set(ctx, getCoinAccountKey(operation.Account), encodedResult); err != nil {
			return fmt.Errorf("%w: unable to set coin account", err)
		}
	}

	return nil
}

func (c *CoinStorage) tryRemovingCoin(ctx context.Context, transaction DatabaseTransaction, blockTransaction *types.Transaction, operation *types.Operation, identiferKey string) error {
	rawIdentifier, ok := operation.Metadata[identiferKey]
	if ok {
		coinIdentifier, ok := rawIdentifier.(string)
		if !ok {
			return fmt.Errorf("unable to parse spent coin %v", rawIdentifier)
		}

		exists, _, err := transaction.Get(ctx, getCoinKey(coinIdentifier))
		if err != nil {
			return fmt.Errorf("%w: unable to query for coin", err)
		}

		if !exists { // this could occur if coin was created before we started syncing
			return nil
		}

		if err := transaction.Delete(ctx, getCoinKey(coinIdentifier)); err != nil {
			return fmt.Errorf("%w: unable to delete coin", err)
		}

		accountExists, val, err := transaction.Get(ctx, getCoinAccountKey(operation.Account))
		if err != nil {
			return fmt.Errorf("%w: unable to query coin account", err)
		}

		if !accountExists {
			return fmt.Errorf("%w: unable to find owner of coin", err)
		}

		var coins map[string]struct{}
		if err := decode(val, &coins); err != nil {
			return fmt.Errorf("%w: unable to decode coin account", err)
		}

		if _, exists := coins[coinIdentifier]; !exists {
			return fmt.Errorf("unable to find coin %s in account %s", coinIdentifier, types.PrettyPrintStruct(operation.Account))
		}

		delete(coins, coinIdentifier)

		encodedResult, err := encode(coins)
		if err != nil {
			return fmt.Errorf("%w: unable to encode coins", err)
		}

		if err := transaction.Set(ctx, getCoinAccountKey(operation.Account), encodedResult); err != nil {
			return fmt.Errorf("%w: unable to set coin account", err)
		}
	}

	return nil
}

func (c *CoinStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.Amount == nil {
				continue
			}

			if err := c.tryAddingCoin(ctx, transaction, txn, operation, coinCreated); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, txn, operation, coinSpent); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	return nil, nil
}

func (c *CoinStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.Amount == nil {
				continue
			}

			if err := c.tryAddingCoin(ctx, transaction, txn, operation, coinSpent); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, txn, operation, coinCreated); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	return nil, nil
}

// func (c *CoinStorage) GetCoins(ctx context.Context, accountIdentifier *types.AccountIdentifier) (*Coin, error) {}
