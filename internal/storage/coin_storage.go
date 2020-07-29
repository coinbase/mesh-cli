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
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	coinNamespace        = "coinNamespace"
	coinAccountNamespace = "coinAccountNamespace"
)

var _ BlockWorker = (*CoinStorage)(nil)

// CoinStorage implements storage methods for storing
// UTXOs.
type CoinStorage struct {
	db Database

	asserter *asserter.Asserter
}

// NewCoinStorage returns a new CoinStorage.
func NewCoinStorage(
	db Database,
	asserter *asserter.Asserter,
) *CoinStorage {
	return &CoinStorage{
		db:       db,
		asserter: asserter,
	}
}

// Coin represents some spendable output (typically
// referred to as a UTXO).
type Coin struct {
	Identifier  *types.CoinIdentifier `json:"identifier"`
	Transaction *types.Transaction    `json:"transaction"`
	Operation   *types.Operation      `json:"operation"`
}

func getCoinKey(identifier *types.CoinIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinNamespace, identifier.Identifier))
}

func getCoinAccountKey(accountIdentifier *types.AccountIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinAccountNamespace, types.Hash(accountIdentifier)))
}

func getAndDecodeCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	coinIdentifier *types.CoinIdentifier,
) (bool, *Coin, error) {
	exists, val, err := transaction.Get(ctx, getCoinKey(coinIdentifier))
	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to query for coin", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return false, nil, nil
	}

	var coin Coin
	if err := decode(val, &coin); err != nil {
		return false, nil, fmt.Errorf("%w: unable to decode coin", err)
	}

	return true, &coin, nil
}

func (c *CoinStorage) tryAddingCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockTransaction *types.Transaction,
	operation *types.Operation,
	action types.CoinAction,
) error {
	if operation.CoinChange == nil {
		return errors.New("coin change cannot be nil")
	}

	if operation.CoinChange.CoinAction != action {
		return nil
	}

	coinIdentifier := operation.CoinChange.CoinIdentifier

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

	accountExists, coins, err := getAndDecodeCoins(ctx, transaction, operation.Account)
	if err != nil {
		return fmt.Errorf("%w: unable to query coin account", err)
	}

	if !accountExists {
		coins = map[string]struct{}{}
	}

	if _, exists := coins[coinIdentifier.Identifier]; exists {
		return fmt.Errorf(
			"coin %s already exists in account %s",
			coinIdentifier,
			types.PrettyPrintStruct(operation.Account),
		)
	}

	coins[coinIdentifier.Identifier] = struct{}{}

	if err := encodeAndSetCoins(ctx, transaction, operation.Account, coins); err != nil {
		return fmt.Errorf("%w: unable to set coin account", err)
	}

	return nil
}

func encodeAndSetCoins(
	ctx context.Context,
	transaction DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
	coins map[string]struct{},
) error {
	encodedResult, err := encode(coins)
	if err != nil {
		return fmt.Errorf("%w: unable to encode coins", err)
	}

	if err := transaction.Set(ctx, getCoinAccountKey(accountIdentifier), encodedResult); err != nil {
		return fmt.Errorf("%w: unable to set coin account", err)
	}

	return nil
}

func getAndDecodeCoins(
	ctx context.Context,
	transaction DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
) (bool, map[string]struct{}, error) {
	accountExists, val, err := transaction.Get(ctx, getCoinAccountKey(accountIdentifier))
	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to query coin account", err)
	}

	if !accountExists {
		return false, nil, nil
	}

	var coins map[string]struct{}
	if err := decode(val, &coins); err != nil {
		return false, nil, fmt.Errorf("%w: unable to decode coin account", err)
	}

	return true, coins, nil
}

func (c *CoinStorage) tryRemovingCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	operation *types.Operation,
	action types.CoinAction,
) error {
	if operation.CoinChange == nil {
		return errors.New("coin change cannot be nil")
	}

	if operation.CoinChange.CoinAction != action {
		return nil
	}

	coinIdentifier := operation.CoinChange.CoinIdentifier

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

	accountExists, coins, err := getAndDecodeCoins(ctx, transaction, operation.Account)
	if err != nil {
		return fmt.Errorf("%w: unable to query coin account", err)
	}

	if !accountExists {
		return fmt.Errorf("%w: unable to find owner of coin", err)
	}

	if _, exists := coins[coinIdentifier.Identifier]; !exists {
		return fmt.Errorf(
			"unable to find coin %s in account %s",
			coinIdentifier,
			types.PrettyPrintStruct(operation.Account),
		)
	}

	delete(coins, coinIdentifier.Identifier)

	if err := encodeAndSetCoins(ctx, transaction, operation.Account, coins); err != nil {
		return fmt.Errorf("%w: unable to set coin account", err)
	}

	return nil
}

// AddingBlock is called by BlockStorage when adding a block.
func (c *CoinStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.CoinChange == nil {
				continue
			}

			if operation.Amount == nil {
				continue
			}

			success, err := c.asserter.OperationSuccessful(operation)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to parse operation success", err)
			}

			if !success {
				continue
			}

			if err := c.tryAddingCoin(ctx, transaction, txn, operation, types.CoinCreated); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, operation, types.CoinSpent); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	return nil, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
func (c *CoinStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.CoinChange == nil {
				continue
			}

			if operation.Amount == nil {
				continue
			}

			success, err := c.asserter.OperationSuccessful(operation)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to parse operation success", err)
			}

			if !success {
				continue
			}

			// We add spent coins and remove created coins during a re-org (opposite of
			// AddingBlock).
			if err := c.tryAddingCoin(ctx, transaction, txn, operation, types.CoinSpent); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, operation, types.CoinCreated); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	return nil, nil
}

// GetCoins returns all unspent coins for a provided *types.AccountIdentifier.
func (c *CoinStorage) GetCoins(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) ([]*Coin, error) {
	transaction := c.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	accountExists, coins, err := getAndDecodeCoins(ctx, transaction, accountIdentifier)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to query account identifier", err)
	}

	if !accountExists {
		return []*Coin{}, nil
	}

	coinArr := []*Coin{}
	for coinIdentifier := range coins {
		exists, coin, err := getAndDecodeCoin(
			ctx,
			transaction,
			&types.CoinIdentifier{Identifier: coinIdentifier},
		)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to query coin", err)
		}

		if !exists {
			return nil, fmt.Errorf("%w: unable to get coin %s", err, coinIdentifier)
		}

		coinArr = append(coinArr, coin)
	}

	return coinArr, nil
}

// GetLargestCoin returns the largest Coin for a
// *types.AccountIdentifier and *types.Currency.
// If no Coins are available, a 0 balance is returned.
func (c *CoinStorage) GetLargestCoin(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*big.Int, *types.CoinIdentifier, error) {
	coins, err := c.GetCoins(ctx, accountIdentifier)
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
