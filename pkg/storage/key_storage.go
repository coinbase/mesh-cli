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
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// WARNING: KEY STORAGE USING THIS PACKAGE IS NOT SECURE!!!! ONLY USE
// FOR TESTING!!!!

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	keyNamespace = "key"
)

func getAddressKey(address string) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", keyNamespace, address),
	)
}

// KeyStorage implements key storage methods
// on top of a Database and DatabaseTransaction interface.
type KeyStorage struct {
	db Database
}

// NewKeyStorage returns a new KeyStorage.
func NewKeyStorage(
	db Database,
) *KeyStorage {
	return &KeyStorage{
		db: db,
	}
}

type key struct {
	Address string        `json:"address"`
	KeyPair *keys.KeyPair `json:"keypair"`
}

func parseKey(buf []byte) (*key, error) {
	var k key
	err := getDecoder(bytes.NewReader(buf)).Decode(&k)
	if err != nil {
		return nil, err
	}

	return &k, nil
}

func serializeKey(k *key) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := getEncoder(buf).Encode(k)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Store saves a keys.KeyPair for a given address. If the address already
// exists, an error is returned.
func (k *KeyStorage) Store(ctx context.Context, address string, keyPair *keys.KeyPair) error {
	transaction := k.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	exists, _, err := transaction.Get(ctx, getAddressKey(address))
	if err != nil {
		return fmt.Errorf("%w: unable to check if address %s exists", err, address)
	}

	if exists {
		return fmt.Errorf("address %s already exists", address)
	}

	val, err := serializeKey(&key{
		Address: address,
		KeyPair: keyPair,
	})
	if err != nil {
		return fmt.Errorf("%w: unable to serialize key", err)
	}

	err = transaction.Set(ctx, getAddressKey(address), val)
	if err != nil {
		return fmt.Errorf("%w: unable to store key", err)
	}

	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit new key to db", err)
	}

	return nil
}

// Get returns a *keys.KeyPair for an address, if it exists.
func (k *KeyStorage) Get(ctx context.Context, address string) (*keys.KeyPair, error) {
	transaction := k.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	exists, rawKey, err := transaction.Get(ctx, getAddressKey(address))
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get address %s", err, address)
	}

	if !exists {
		return nil, fmt.Errorf("address not found %s", address)
	}

	key, err := parseKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to parse saved key", err)
	}

	return key.KeyPair, nil
}

// GetAllAddresses returns all addresses in key storage.
func (k *KeyStorage) GetAllAddresses(ctx context.Context) ([]string, error) {
	rawKeys, err := k.db.Scan(ctx, []byte(keyNamespace))
	if err != nil {
		return nil, fmt.Errorf("%w database scan for keys failed", err)
	}

	addresses := make([]string, len(rawKeys))
	for i, rawKey := range rawKeys {
		kp, err := parseKey(rawKey)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to parse key pair", err)
		}

		addresses[i] = kp.Address
	}

	return addresses, nil
}

// Sign attempts to sign a slice of *types.SigningPayload with the keys in KeyStorage.
func (k *KeyStorage) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	signatures := make([]*types.Signature, len(payloads))
	for i, payload := range payloads {
		keyPair, err := k.Get(ctx, payload.Address)
		if err != nil {
			return nil, fmt.Errorf("%w: could not get key for %s", err, payload.Address)
		}

		signer, err := keyPair.Signer()
		if err != nil {
			return nil, fmt.Errorf("%w: unable to create signer", err)
		}

		if len(payload.SignatureType) == 0 {
			return nil, fmt.Errorf("cannot determine signature type for payload %d", i)
		}

		signature, err := signer.Sign(payload, payload.SignatureType)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to to sign payload %d", err, i)
		}

		signatures[i] = signature
	}

	return signatures, nil
}

// RandomAddress returns a random address from all addresses.
func (k *KeyStorage) RandomAddress(ctx context.Context) (string, error) {
	addresses, err := k.GetAllAddresses(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get addresses", err)
	}

	if len(addresses) == 0 {
		return "", errors.New("no addresses available")
	}

	return addresses[rand.Intn(len(addresses))], nil
}
