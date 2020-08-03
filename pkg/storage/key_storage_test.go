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

	"github.com/slowboat0/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func hash(message string) []byte {
	messageHashBytes := common.BytesToHash([]byte(message)).Bytes()
	return messageHashBytes
}

func TestKeyStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	k := NewKeyStorage(database)

	kp1, err := keys.GenerateKeypair(types.Edwards25519)
	assert.NoError(t, err)

	kp2, err := keys.GenerateKeypair(types.Secp256k1)
	assert.NoError(t, err)

	t.Run("get non-existent key", func(t *testing.T) {
		v, err := k.Get(ctx, "blah")
		assert.Error(t, err)
		assert.Nil(t, v)

		addrs, err := k.GetAllAddresses(ctx)
		assert.NoError(t, err)
		assert.Len(t, addrs, 0)
	})

	t.Run("store and get key", func(t *testing.T) {
		err = k.Store(ctx, "addr1", kp1)
		assert.NoError(t, err)

		v, err := k.Get(ctx, "addr1")
		assert.NoError(t, err)
		assert.Equal(t, kp1, v)

		addrs, err := k.GetAllAddresses(ctx)
		assert.NoError(t, err)
		assert.Equal(t, []string{"addr1"}, addrs)
	})

	t.Run("attempt overwrite", func(t *testing.T) {
		err = k.Store(ctx, "addr1", kp2)
		assert.Error(t, err)

		v, err := k.Get(ctx, "addr1")
		assert.NoError(t, err)
		assert.Equal(t, kp1, v)
	})

	t.Run("store and get another key", func(t *testing.T) {
		err = k.Store(ctx, "addr2", kp2)
		assert.NoError(t, err)

		v, err := k.Get(ctx, "addr2")
		assert.NoError(t, err)
		assert.Equal(t, kp2, v)

		addrs, err := k.GetAllAddresses(ctx)
		assert.NoError(t, err)
		assert.Equal(t, []string{"addr1", "addr2"}, addrs)
	})

	t.Run("sign payloads", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				Address:       "addr1",
				Bytes:         hash("msg1"),
				SignatureType: types.Ed25519,
			},
			{
				Address:       "addr2",
				Bytes:         hash("msg2"),
				SignatureType: types.Ecdsa,
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.NoError(t, err)
		assert.Len(t, sigs, 2)
		assert.NoError(t, (&keys.SignerEdwards25519{}).Verify(sigs[0]))
		assert.NoError(t, (&keys.SignerSecp256k1{}).Verify(sigs[1]))
	})

	t.Run("missing address in sign", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				Address:       "addr3",
				Bytes:         hash("msg3"),
				SignatureType: types.Ed25519,
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.Error(t, err)
		assert.Nil(t, sigs)
	})

	t.Run("missing signature type in sign", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				Address: "addr1",
				Bytes:   hash("msg1"),
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.Error(t, err)
		assert.Nil(t, sigs)
	})
}
