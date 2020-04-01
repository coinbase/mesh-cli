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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	t.Run("No key exists", func(t *testing.T) {
		exists, value, err := database.Get(ctx, []byte("hello"))
		assert.False(t, exists)
		assert.Nil(t, value)
		assert.NoError(t, err)
	})

	t.Run("Set key", func(t *testing.T) {
		err := database.Set(ctx, []byte("hello"), []byte("hola"))
		assert.NoError(t, err)
	})

	t.Run("Get key", func(t *testing.T) {
		exists, value, err := database.Get(ctx, []byte("hello"))
		assert.True(t, exists)
		assert.Equal(t, []byte("hola"), value)
		assert.NoError(t, err)
	})
}

func TestDatabaseTransaction(t *testing.T) {
	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	t.Run("Set and get within a transaction", func(t *testing.T) {
		txn := database.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, txn.Set(ctx, []byte("hello"), []byte("hola")))

		// Ensure tx does not affect db
		exists, value, err := database.Get(ctx, []byte("hello"))
		assert.False(t, exists)
		assert.Nil(t, value)
		assert.NoError(t, err)

		assert.NoError(t, txn.Commit(ctx))

		exists, value, err = database.Get(ctx, []byte("hello"))
		assert.True(t, exists)
		assert.Equal(t, []byte("hola"), value)
		assert.NoError(t, err)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := database.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, txn.Set(ctx, []byte("hello"), []byte("world")))

		txn.Discard(ctx)

		exists, value, err := database.Get(ctx, []byte("hello"))
		assert.True(t, exists)
		assert.Equal(t, []byte("hola"), value)
		assert.NoError(t, err)
	})

	t.Run("Delete within a transaction", func(t *testing.T) {
		txn := database.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, txn.Delete(ctx, []byte("hello")))
		assert.NoError(t, txn.Commit(ctx))

		exists, value, err := database.Get(ctx, []byte("hello"))
		assert.False(t, exists)
		assert.Nil(t, value)
		assert.NoError(t, err)
	})
}
