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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/big"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"

	"github.com/davecgh/go-spew/spew"
)

var (
	// ErrHeadBlockNotFound is returned when there is no
	// head block found in BlockStorage.
	ErrHeadBlockNotFound = errors.New("Head block not found")

	// ErrBlockNotFound is returned when a block is not
	// found in BlockStorage.
	ErrBlockNotFound = errors.New("Block not found")

	// ErrAccountNotFound is returned when an account
	// is not found in BlockStorage.
	ErrAccountNotFound = errors.New("Account not found")

	// ErrNegativeBalance is returned when an account
	// balance goes negative as the result of an operation.
	ErrNegativeBalance = errors.New("Negative balance")

	// ErrDuplicateBlockHash is returned when a block hash
	// cannot be stored because it is a duplicate.
	ErrDuplicateBlockHash = errors.New("Duplicate block hash")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("Duplicate transaction hash")
)

const (
	// headBlockKey is used to lookup the head block identifier.
	// The head block is the block with the largest index that is
	// not orphaned.
	headBlockKey = "head-block"

	// blockHashNamespace is prepended to any stored block hash.
	// We cannot just use the stored block key to lookup whether
	// a hash has been used before because it is concatenated
	// with the index of the stored block.
	blockHashNamespace = "block-hash"

	// transactionHashNamespace is prepended to any stored
	// transaction hash.
	transactionHashNamespace = "transaction-hash"

	// balanceNamespace is prepended to any stored balance.
	balanceNamespace = "balance"
)

/*
  Key Construction
*/

// hashBytes is used to construct a SHA1
// hash to protect against arbitrarily
// large key sizes.
func hashBytes(data []byte) []byte {
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		log.Fatal(err)
	}

	return h.Sum(nil)
}

// hashString is used to construct a SHA1
// hash to protect against arbitrarily
// large key sizes.
func hashString(data string) string {
	return fmt.Sprintf("%x", hashBytes([]byte(data)))
}

func getHeadBlockKey() []byte {
	return hashBytes([]byte(headBlockKey))
}

func getBlockKey(blockIdentifier *rosetta.BlockIdentifier) []byte {
	return hashBytes(
		[]byte(fmt.Sprintf("%s:%d", blockIdentifier.Hash, blockIdentifier.Index)),
	)
}

func getHashKey(hash string, isBlock bool) []byte {
	if isBlock {
		return hashBytes([]byte(fmt.Sprintf("%s:%s", blockHashNamespace, hash)))
	}

	return hashBytes([]byte(fmt.Sprintf("%s:%s", transactionHashNamespace, hash)))
}

func getBalanceKey(account *rosetta.AccountIdentifier) []byte {
	if account.SubAccount == nil {
		return hashBytes(
			[]byte(fmt.Sprintf("%s:%s", balanceNamespace, account.Address)),
		)
	}

	if account.SubAccount.Metadata == nil {
		return hashBytes([]byte(fmt.Sprintf(
			"%s:%s:%s",
			balanceNamespace,
			account.Address,
			account.SubAccount.SubAccount,
		)))
	}

	// TODO: handle SubAccount.Metadata
	// that contains pointer values.
	return hashBytes([]byte(fmt.Sprintf(
		"%s:%s:%s:%v",
		balanceNamespace,
		account.Address,
		account.SubAccount.SubAccount,
		*account.SubAccount.Metadata,
	)))
}

// BlockStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type BlockStorage struct {
	db Database
}

// NewBlockStorage returns a new BlockStorage.
func NewBlockStorage(ctx context.Context, db Database) *BlockStorage {
	return &BlockStorage{
		db: db,
	}
}

// NewDatabaseTransaction returns a DatabaseTransaction
// from the Database that is backing BlockStorage.
func (b *BlockStorage) NewDatabaseTransaction(
	ctx context.Context,
	write bool,
) DatabaseTransaction {
	return b.db.NewDatabaseTransaction(ctx, write)
}

// GetHeadBlockIdentifier returns the head block identifier,
// if it exists.
func (b *BlockStorage) GetHeadBlockIdentifier(
	ctx context.Context,
	transaction DatabaseTransaction,
) (*rosetta.BlockIdentifier, error) {
	exists, block, err := transaction.Get(ctx, getHeadBlockKey())
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrHeadBlockNotFound
	}

	dec := gob.NewDecoder(bytes.NewReader(block))
	var blockIdentifier rosetta.BlockIdentifier
	err = dec.Decode(&blockIdentifier)
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
	blockIdentifier *rosetta.BlockIdentifier,
) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(blockIdentifier)
	if err != nil {
		return err
	}

	return transaction.Set(ctx, getHeadBlockKey(), buf.Bytes())
}

// GetBlock returns a block, if it exists.
func (b *BlockStorage) GetBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *rosetta.BlockIdentifier,
) (*rosetta.Block, error) {
	exists, block, err := transaction.Get(ctx, getBlockKey(blockIdentifier))
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w %+v", ErrBlockNotFound, blockIdentifier)
	}

	var rosettaBlock rosetta.Block
	err = gob.NewDecoder(bytes.NewBuffer(block)).Decode(&rosettaBlock)
	if err != nil {
		return nil, err
	}

	return &rosettaBlock, nil
}

// storeHash stores either a block or transaction hash.
func (b *BlockStorage) storeHash(
	ctx context.Context,
	transaction DatabaseTransaction,
	hash string,
	isBlock bool,
) error {
	key := getHashKey(hash, isBlock)
	exists, _, err := transaction.Get(ctx, key)
	if err != nil {
		return err
	}

	if !exists {
		return transaction.Set(ctx, key, []byte(""))
	}

	if isBlock {
		return fmt.Errorf(
			"%w %s",
			ErrDuplicateBlockHash,
			hash,
		)
	}

	return fmt.Errorf(
		"%w %s",
		ErrDuplicateTransactionHash,
		hash,
	)
}

// StoreBlock stores a block or returns an error.
// StoreBlock also stores the block hash and all
// its transaction hashes for duplicate detection.
func (b *BlockStorage) StoreBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	block *rosetta.Block,
) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(block)
	if err != nil {
		return err
	}

	// Store block
	err = transaction.Set(ctx, getBlockKey(block.BlockIdentifier), buf.Bytes())
	if err != nil {
		return err
	}

	// Store block hash
	err = b.storeHash(ctx, transaction, block.BlockIdentifier.Hash, true)
	if err != nil {
		return err
	}

	// Store all transaction hashes
	for _, txn := range block.Transactions {
		err = b.storeHash(ctx, transaction, txn.TransactionIdentifier.Hash, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveBlock removes a block or returns an error.
// RemoveBlock also removes the block hash and all
// its transaction hashes to not break duplicate
// detection. This is called within a re-org.
func (b *BlockStorage) RemoveBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	block *rosetta.BlockIdentifier,
) error {
	// Remove all transaction hashes
	blockData, err := b.GetBlock(ctx, transaction, block)
	for _, txn := range blockData.Transactions {
		err = transaction.Delete(ctx, getHashKey(txn.TransactionIdentifier.Hash, false))
		if err != nil {
			return err
		}
	}

	// Remove block hash
	err = transaction.Delete(ctx, getHashKey(block.Hash, true))
	if err != nil {
		return err
	}

	// Remove block
	return transaction.Delete(ctx, getBlockKey(block))
}

type balanceEntry struct {
	Amounts map[string]*rosetta.Amount
	Block   *rosetta.BlockIdentifier
}

func serializeBalanceEntry(bal balanceEntry) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(bal)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func parseBalanceEntry(buf []byte) (*balanceEntry, error) {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	var bal balanceEntry
	err := dec.Decode(&bal)
	if err != nil {
		return nil, err
	}

	return &bal, nil
}

// GetCurrencyKey is used to identify a *rosetta.Currency
// in an account's map of currencies. It is not feasible
// to create a map of [rosetta.Currency]*rosetta.Amount
// because rosetta.Currency contains a metadata pointer
// that would prevent any equality.
func GetCurrencyKey(currency *rosetta.Currency) string {
	if currency.Metadata == nil {
		return hashString(
			fmt.Sprintf("%s:%d", currency.Symbol, currency.Decimals),
		)
	}

	// TODO: Handle currency.Metadata
	// that has pointer value.
	return hashString(
		fmt.Sprintf(
			"%s:%d:%v",
			currency.Symbol,
			currency.Decimals,
			*currency.Metadata,
		),
	)
}

// UpdateBalance updates a rosetta.AccountIdentifer
// by a rosetta.Amount and sets the account's most
// recent accessed block.
func (b *BlockStorage) UpdateBalance(
	ctx context.Context,
	transaction DatabaseTransaction,
	account *rosetta.AccountIdentifier,
	amount *rosetta.Amount,
	block *rosetta.BlockIdentifier,
) error {
	if amount == nil || amount.Currency == nil {
		return errors.New("invalid amount")
	}

	key := getBalanceKey(account)
	// Get existing balance on key
	exists, balance, err := transaction.Get(ctx, key)
	if err != nil {
		return err
	}

	currencyKey := GetCurrencyKey(amount.Currency)

	if !exists {
		amountMap := make(map[string]*rosetta.Amount)
		newVal, ok := new(big.Int).SetString(amount.Value, 10)
		if !ok {
			return fmt.Errorf("%s is not an integer", amount.Value)
		}
		if newVal.Sign() == -1 {
			return fmt.Errorf(
				"%w %+v for %+v at %+v",
				ErrNegativeBalance,
				spew.Sdump(amount),
				account,
				block,
			)
		}
		amountMap[currencyKey] = amount

		serialBal, err := serializeBalanceEntry(balanceEntry{
			Amounts: amountMap,
			Block:   block,
		})
		if err != nil {
			return err
		}

		return transaction.Set(ctx, key, serialBal)
	}

	// Modify balance
	parseBal, err := parseBalanceEntry(balance)
	if err != nil {
		return err
	}

	val, ok := parseBal.Amounts[currencyKey]
	if !ok {
		parseBal.Amounts[currencyKey] = amount
	}

	modification, ok := new(big.Int).SetString(amount.Value, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", amount.Value)
	}

	existing, ok := new(big.Int).SetString(val.Value, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", val.Value)
	}

	newVal := new(big.Int).Add(existing, modification)
	val.Value = newVal.String()
	if newVal.Sign() == -1 {
		return fmt.Errorf(
			"%w %+v for %+v at %+v",
			ErrNegativeBalance,
			spew.Sdump(val),
			account,
			block,
		)
	}

	parseBal.Amounts[currencyKey] = val

	parseBal.Block = block
	serialBal, err := serializeBalanceEntry(*parseBal)
	if err != nil {
		return err
	}
	return transaction.Set(ctx, key, serialBal)
}

// GetBalance returns all the balances of a rosetta.AccountIdentifier
// and the rosetta.BlockIdentifier it was last updated at.
func (b *BlockStorage) GetBalance(
	ctx context.Context,
	transaction DatabaseTransaction,
	account *rosetta.AccountIdentifier,
) (map[string]*rosetta.Amount, *rosetta.BlockIdentifier, error) {
	key := getBalanceKey(account)
	exists, bal, err := transaction.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	if !exists {
		return nil, nil, fmt.Errorf("%w %+v", ErrAccountNotFound, account)
	}

	deserialBal, err := parseBalanceEntry(bal)
	if err != nil {
		return nil, nil, err
	}

	return deserialBal.Amounts, deserialBal.Block, nil
}
