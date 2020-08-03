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

	"github.com/dgraph-io/badger"
)

// BadgerStorage is a wrapper around Badger DB
// that implements the Database interface.
type BadgerStorage struct {
	db *badger.DB
}

// NewBadgerStorage creates a new BadgerStorage.
func NewBadgerStorage(ctx context.Context, dir string) (Database, error) {
	options := badger.DefaultOptions(dir)
	options.Logger = nil
	db, err := badger.Open(options)
	if err != nil {
		return nil, fmt.Errorf("%w could not open badger database", err)
	}

	return &BadgerStorage{
		db: db,
	}, nil
}

// Close closes the database to prevent corruption.
// The caller should defer this in main.
func (b *BadgerStorage) Close(ctx context.Context) error {
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("%w unable to close database", err)
	}

	return nil
}

// BadgerTransaction is a wrapper around a Badger
// DB transaction that implements the DatabaseTransaction
// interface.
type BadgerTransaction struct {
	txn *badger.Txn
}

// NewDatabaseTransaction creates a new BadgerTransaction.
// If the transaction will not modify any values, pass
// in false for the write parameter (this allows for
// optimization within the Badger DB).
func (b *BadgerStorage) NewDatabaseTransaction(
	ctx context.Context,
	write bool,
) DatabaseTransaction {
	return &BadgerTransaction{
		txn: b.db.NewTransaction(write),
	}
}

// Commit attempts to commit and discard the transaction.
func (b *BadgerTransaction) Commit(context.Context) error {
	return b.txn.Commit()
}

// Discard discards an open transaction. All transactions
// must be either discarded or committed.
func (b *BadgerTransaction) Discard(context.Context) {
	b.txn.Discard()
}

// Set changes the value of the key to the value within a transaction.
func (b *BadgerTransaction) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.txn.Set(key, value)
}

// Get accesses the value of the key within a transaction.
func (b *BadgerTransaction) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	err = item.Value(func(v []byte) error {
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Delete removes the key and its value within the transaction.
func (b *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	return b.txn.Delete(key)
}

// Set changes the value of the key to the value in its own transaction.
func (b *BadgerStorage) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get fetches the value of a key in its own transaction.
func (b *BadgerStorage) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(v []byte) error {
			value = make([]byte, len(v))
			copy(value, v)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Scan fetches all items at a given prefix. This is typically
// used to get all items in a namespace.
func (b *BadgerStorage) Scan(
	ctx context.Context,
	prefix []byte,
) ([][]byte, error) {
	values := [][]byte{}
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// There is some strange issue with BadgerDB where the value returned
			// on the item is sometimes corrupted (which causes decoding errors).
			// Until this is fixed, the workaround I found was to fetch the value
			// in a separate transaction with the key from the scan. Instead of
			// spending more time investigating these issues, we will work
			// on prioritizing a switch to a different backend.
			exists, v, err := b.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("%w unable to get key %s", err, string(key))
			}
			if !exists {
				return fmt.Errorf("key %s does not exist", string(key))
			}

			values = append(values, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return values, nil
}
