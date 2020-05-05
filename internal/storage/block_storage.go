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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"path"

	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/types"
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

var (
	// ErrHeadBlockNotFound is returned when there is no
	// head block found in BlockStorage.
	ErrHeadBlockNotFound = errors.New("head block not found")

	// ErrBlockNotFound is returned when a block is not
	// found in BlockStorage.
	ErrBlockNotFound = errors.New("block not found")

	// ErrAccountNotFound is returned when an account
	// is not found in BlockStorage.
	ErrAccountNotFound = errors.New("account not found")

	// ErrNegativeBalance is returned when an account
	// balance goes negative as the result of an operation.
	ErrNegativeBalance = errors.New("negative balance")

	// ErrDuplicateBlockHash is returned when a block hash
	// cannot be stored because it is a duplicate.
	ErrDuplicateBlockHash = errors.New("duplicate block hash")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("duplicate transaction hash")

	// ErrAlreadyStartedSyncing is returned when trying to bootstrap
	// balances after syncing has started.
	ErrAlreadyStartedSyncing = errors.New("cannot bootstrap accounts, already started syncing")
)

/*
  Key Construction
*/

// hashBytes is used to construct a SHA1
// hash to protect against arbitrarily
// large key sizes.
func hashBytes(data string) []byte {
	h := sha256.New()
	_, err := h.Write([]byte(data))
	if err != nil {
		log.Fatal(err)
	}

	return h.Sum(nil)
}

func getHeadBlockKey() []byte {
	return hashBytes(headBlockKey)
}

func getBlockKey(blockIdentifier *types.BlockIdentifier) []byte {
	return hashBytes(
		fmt.Sprintf("%s:%d", blockIdentifier.Hash, blockIdentifier.Index),
	)
}

func getHashKey(hash string, isBlock bool) []byte {
	if isBlock {
		return hashBytes(fmt.Sprintf("%s:%s", blockHashNamespace, hash))
	}

	return hashBytes(fmt.Sprintf("%s:%s", transactionHashNamespace, hash))
}

// GetCurrencyKey is used to identify a *types.Currency
// in an account's map of currencies. It is not feasible
// to create a map of [types.Currency]*types.Amount
// because types.Currency contains a metadata pointer
// that would prevent any equality.
func GetCurrencyKey(currency *types.Currency) string {
	if currency.Metadata == nil {
		return fmt.Sprintf("%s:%d", currency.Symbol, currency.Decimals)
	}

	// TODO: Handle currency.Metadata
	// that has pointer value.
	return fmt.Sprintf(
		"%s:%d:%v",
		currency.Symbol,
		currency.Decimals,
		currency.Metadata,
	)
}

// GetAccountKey returns a byte slice representing a *types.AccountIdentifier.
// This byte slice automatically handles the existence of *types.SubAccount
// detail.
func GetAccountKey(account *types.AccountIdentifier) string {
	if account.SubAccount == nil {
		return fmt.Sprintf("%s:%s", balanceNamespace, account.Address)
	}

	if account.SubAccount.Metadata == nil {
		return fmt.Sprintf(
			"%s:%s:%s",
			balanceNamespace,
			account.Address,
			account.SubAccount.Address,
		)
	}

	// TODO: handle SubAccount.Metadata
	// that contains pointer values.
	return fmt.Sprintf(
		"%s:%s:%s:%v",
		balanceNamespace,
		account.Address,
		account.SubAccount.Address,
		account.SubAccount.Metadata,
	)
}

func GetBalanceKey(account *types.AccountIdentifier, currency *types.Currency) []byte {
	return hashBytes(
		fmt.Sprintf("%s/%s", GetAccountKey(account), GetCurrencyKey(currency)),
	)
}

type BlockStorageHelper interface {
	AccountBalance(
		ctx context.Context,
		account *types.AccountIdentifier,
		currency *types.Currency,
		block *types.BlockIdentifier,
	) (*types.Amount, error) // returns an error if lookupBalanceByBlock disabled

	SkipOperation(
		ctx context.Context,
		op *types.Operation,
	) (bool, error)
}

// BlockStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type BlockStorage struct {
	db     Database
	helper BlockStorageHelper
}

// NewBlockStorage returns a new BlockStorage.
func NewBlockStorage(
	ctx context.Context,
	db Database,
	helper BlockStorageHelper,
) *BlockStorage {
	return &BlockStorage{
		db:     db,
		helper: helper,
	}
}

// NewDatabaseTransaction returns a DatabaseTransaction
// from the Database that is backing BlockStorage.
func (b *BlockStorage) newDatabaseTransaction(
	ctx context.Context,
	write bool,
) DatabaseTransaction {
	return b.db.NewDatabaseTransaction(ctx, write)
}

// GetHeadBlockIdentifier returns the head block identifier,
// if it exists.
func (b *BlockStorage) GetHeadBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	transaction := b.newDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	exists, block, err := transaction.Get(ctx, getHeadBlockKey())
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrHeadBlockNotFound
	}

	dec := gob.NewDecoder(bytes.NewReader(block))
	var blockIdentifier types.BlockIdentifier
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
	blockIdentifier *types.BlockIdentifier,
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
	blockIdentifier *types.BlockIdentifier,
) (*types.Block, error) {
	transaction := b.newDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	exists, block, err := transaction.Get(ctx, getBlockKey(blockIdentifier))
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w %+v", ErrBlockNotFound, blockIdentifier)
	}

	var rosettaBlock types.Block
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
	block *types.Block,
) ([]*reconciler.BalanceChange, error) {
	transaction := b.newDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(block)
	if err != nil {
		return nil, err
	}

	// Store block
	err = transaction.Set(ctx, getBlockKey(block.BlockIdentifier), buf.Bytes())
	if err != nil {
		return nil, err
	}

	// Store block hash
	err = b.storeHash(ctx, transaction, block.BlockIdentifier.Hash, true)
	if err != nil {
		return nil, err
	}

	// Store all transaction hashes
	for _, txn := range block.Transactions {
		err = b.storeHash(ctx, transaction, txn.TransactionIdentifier.Hash, false)
		if err != nil {
			return nil, err
		}
	}

	changes, err := b.BalanceChanges(ctx, block, false)
	if err != nil {
		return nil, err
	}

	for _, change := range changes {
		if err := b.UpdateBalance(ctx, transaction, change, block.ParentBlockIdentifier); err != nil {
			return nil, err
		}
	}

	if err = b.StoreHeadBlockIdentifier(ctx, transaction, block.BlockIdentifier); err != nil {
		return nil, err
	}

	if err := transaction.Commit(ctx); err != nil {
		return nil, err
	}

	return changes, nil
}

// RemoveBlock removes a block or returns an error.
// RemoveBlock also removes the block hash and all
// its transaction hashes to not break duplicate
// detection. This is called within a re-org.
func (b *BlockStorage) RemoveBlock(
	ctx context.Context,
	block *types.Block,
) ([]*reconciler.BalanceChange, error) {
	transaction := b.newDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	changes, err := b.BalanceChanges(ctx, block, true)
	if err != nil {
		return nil, err
	}

	for _, change := range changes {
		if err := b.UpdateBalance(ctx, transaction, change, block.BlockIdentifier); err != nil {
			return nil, err
		}
	}

	// Remove all transaction hashes
	for _, txn := range block.Transactions {
		err = transaction.Delete(ctx, getHashKey(txn.TransactionIdentifier.Hash, false))
		if err != nil {
			return nil, err
		}
	}

	// Remove block hash
	err = transaction.Delete(ctx, getHashKey(block.BlockIdentifier.Hash, true))
	if err != nil {
		return nil, err
	}

	// Remove block
	if err := transaction.Delete(ctx, getBlockKey(block.BlockIdentifier)); err != nil {
		return nil, err
	}

	if err = b.StoreHeadBlockIdentifier(ctx, transaction, block.ParentBlockIdentifier); err != nil {
		return nil, err
	}

	if err := transaction.Commit(ctx); err != nil {
		return nil, err
	}

	return changes, nil
}

type balanceEntry struct {
	Amount *types.Amount
	Block  *types.BlockIdentifier
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

	currBlock := head
	for currBlock.Index > startIndex {
		block, err := b.GetBlock(ctx, head)
		if err != nil {
			return err
		}

		if _, err = b.RemoveBlock(ctx, block); err != nil {
			return err
		}

		currBlock = block.ParentBlockIdentifier
	}

	return nil
}

func (b *BlockStorage) SetBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	account *types.AccountIdentifier,
	amount *types.Amount,
	block *types.BlockIdentifier,
) error {
	key := GetBalanceKey(account, amount.Currency)

	serialBal, err := serializeBalanceEntry(balanceEntry{
		Amount: amount,
		Block:  block,
	})
	if err != nil {
		return err
	}

	if err := dbTransaction.Set(ctx, key, serialBal); err != nil {
		return err
	}

	return nil
}

// UpdateBalance updates a types.AccountIdentifer
// by a types.Amount and sets the account's most
// recent accessed block.
func (b *BlockStorage) UpdateBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	change *reconciler.BalanceChange,
	parentBlock *types.BlockIdentifier,
) error {
	if change.Currency == nil {
		return errors.New("invalid currency")
	}

	key := GetBalanceKey(change.Account, change.Currency)
	// Get existing balance on key
	exists, balance, err := dbTransaction.Get(ctx, key)
	if err != nil {
		return err
	}

	var existingValue string
	if exists {
		parseBal, err := parseBalanceEntry(balance)
		if err != nil {
			return err
		}

		existingValue = parseBal.Amount.Value
	} else {
		amount, err := b.helper.AccountBalance(ctx, change.Account, change.Currency, parentBlock)
		if err != nil {
			return fmt.Errorf("%w: unable to get previous account balance", err)
		}

		existingValue = amount.Value
	}

	newVal, err := utils.AddStringValues(change.Difference, existingValue)
	if err != nil {
		return err
	}

	bigNewVal, ok := new(big.Int).SetString(newVal, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", newVal)
	}

	if bigNewVal.Sign() == -1 {
		return fmt.Errorf(
			"%w %s:%+v for %+v at %+v",
			ErrNegativeBalance,
			newVal,
			change.Currency,
			change.Account,
			change.Block,
		)
	}

	serialBal, err := serializeBalanceEntry(balanceEntry{
		Amount: &types.Amount{
			Value:    newVal,
			Currency: change.Currency,
		},
		Block: change.Block,
	})
	if err != nil {
		return err
	}

	return dbTransaction.Set(ctx, key, serialBal)
}

// GetBalance returns all the balances of a types.AccountIdentifier
// and the types.BlockIdentifier it was last updated at.
func (b *BlockStorage) GetBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	transaction := b.newDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	key := GetBalanceKey(account, currency)
	exists, bal, err := transaction.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	// When beginning syncing from an arbitrary height, an account may
	// not yet have a cached balance when requested. If this is the case,
	// we fetch the balance from the node for the given height and persist
	// it. This is particularly useful when monitoring interesting accounts.
	if !exists {
		amount, err := b.helper.AccountBalance(ctx, account, currency, headBlock)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get account balance from helper", err)
		}

		writeTransaction := b.newDatabaseTransaction(ctx, true)
		defer writeTransaction.Discard(ctx)
		err = b.SetBalance(
			ctx,
			writeTransaction,
			account,
			amount,
			headBlock,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to set account balance", err)
		}

		err = writeTransaction.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to commit account balance transaction", err)
		}

		return amount, headBlock, nil
	}

	deserialBal, err := parseBalanceEntry(bal)
	if err != nil {
		return nil, nil, err
	}

	return deserialBal.Amount, deserialBal.Block, nil
}

// BootstrapBalance represents a balance of
// a *types.AccountIdentifier and a *types.Currency in the
// genesis block.
// TODO: Must be exported for use
type BootstrapBalance struct {
	Account  *types.AccountIdentifier `json:"account_identifier,omitempty"`
	Currency *types.Currency          `json:"currency,omitempty"`
	Value    string                   `json:"value,omitempty"`
}

// BootstrapBalances is utilized to set the balance of
// any number of AccountIdentifiers at the genesis blocks.
// This is particularly useful for setting the value of
// accounts that received an allocation in the genesis block.
func (b *BlockStorage) BootstrapBalances(
	ctx context.Context,
	bootstrapBalancesFile string,
	genesisBlockIdentifier *types.BlockIdentifier,
) error {
	// Read bootstrap file
	bootstrapBalancesRaw, err := ioutil.ReadFile(path.Clean(bootstrapBalancesFile))
	if err != nil {
		return err
	}

	balances := []*BootstrapBalance{}
	if err := json.Unmarshal(bootstrapBalancesRaw, &balances); err != nil {
		return err
	}

	_, err = b.GetHeadBlockIdentifier(ctx)
	if err != ErrHeadBlockNotFound {
		return ErrAlreadyStartedSyncing
	}

	// Update balances in database
	dbTransaction := b.newDatabaseTransaction(ctx, true)
	defer dbTransaction.Discard(ctx)

	for _, balance := range balances {
		// Ensure change.Difference is valid
		amountValue, ok := new(big.Int).SetString(balance.Value, 10)
		if !ok {
			return fmt.Errorf("%s is not an integer", balance.Value)
		}

		if amountValue.Sign() < 1 {
			return fmt.Errorf("cannot bootstrap zero or negative balance %s", amountValue.String())
		}

		log.Printf(
			"Setting account %s balance to %s %+v\n",
			balance.Account.Address,
			balance.Value,
			balance.Currency,
		)

		err = b.SetBalance(
			ctx,
			dbTransaction,
			balance.Account,
			&types.Amount{
				Value:    balance.Value,
				Currency: balance.Currency,
			},
			genesisBlockIdentifier,
		)
		if err != nil {
			return err
		}
	}

	err = dbTransaction.Commit(ctx)
	if err != nil {
		return err
	}

	log.Printf("%d Balances Bootstrapped\n", len(balances))
	return nil
}

// BalanceChanges returns all balance changes for
// a particular block. All balance changes for a
// particular account are summed into a single
// storage.BalanceChanges struct. If a block is being
// orphaned, the opposite of each balance change is
// returned.
func (b *BlockStorage) BalanceChanges(
	ctx context.Context,
	block *types.Block,
	blockRemoved bool,
) ([]*reconciler.BalanceChange, error) {
	balanceChanges := map[string]*reconciler.BalanceChange{}
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			skip, err := b.helper.SkipOperation(
				ctx,
				op,
			)
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}

			amount := op.Amount
			blockIdentifier := block.BlockIdentifier
			if blockRemoved {
				existing, ok := new(big.Int).SetString(amount.Value, 10)
				if !ok {
					return nil, fmt.Errorf("%s is not an integer", amount.Value)
				}

				amount.Value = new(big.Int).Neg(existing).String()
				blockIdentifier = block.ParentBlockIdentifier
			}

			// Merge values by account and currency
			key := fmt.Sprintf(
				"%x",
				GetBalanceKey(op.Account, op.Amount.Currency),
			)

			val, ok := balanceChanges[key]
			if !ok {
				balanceChanges[key] = &reconciler.BalanceChange{
					Account:    op.Account,
					Currency:   op.Amount.Currency,
					Difference: amount.Value,
					Block:      blockIdentifier,
				}
				continue
			}

			newDifference, err := utils.AddStringValues(val.Difference, amount.Value)
			if err != nil {
				return nil, err
			}
			val.Difference = newDifference
			balanceChanges[key] = val
		}
	}

	allChanges := []*reconciler.BalanceChange{}
	for _, change := range balanceChanges {
		allChanges = append(allChanges, change)
	}

	return allChanges, nil
}
