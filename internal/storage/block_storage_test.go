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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/utils"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestHeadBlockIdentifier(t *testing.T) {
	var (
		newBlockIdentifier = &types.BlockIdentifier{
			Hash:  "blah",
			Index: 0,
		}
		newBlockIdentifier2 = &types.BlockIdentifier{
			Hash:  "blah2",
			Index: 1,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database, &MockBlockStorageHelper{})

	t.Run("No head block set", func(t *testing.T) {
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.EqualError(t, err, ErrHeadBlockNotFound.Error())
		assert.Nil(t, blockIdentifier)
	})

	t.Run("Set and get head block", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier))
		assert.NoError(t, txn.Commit(ctx))

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Discard head block update", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn,
			&types.BlockIdentifier{
				Hash:  "no blah",
				Index: 10,
			}),
		)
		txn.Discard(ctx)

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Multiple updates to head block", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier2))
		assert.NoError(t, txn.Commit(ctx))

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		txn.Discard(ctx)
		assert.Equal(t, newBlockIdentifier2, blockIdentifier)
	})
}

func TestBlock(t *testing.T) {
	var (
		newBlock = &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah",
				Index: 0,
			},
			Timestamp: 1,
			Transactions: []*types.Transaction{
				{
					TransactionIdentifier: &types.TransactionIdentifier{
						Hash: "blahTx",
					},
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 0,
							},
						},
					},
				},
			},
		}

		badBlockIdentifier = &types.BlockIdentifier{
			Hash:  "missing blah",
			Index: 0,
		}

		newBlock2 = &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 2",
				Index: 1,
			},
			Timestamp: 1,
			Transactions: []*types.Transaction{
				{
					TransactionIdentifier: &types.TransactionIdentifier{
						Hash: "blahTx",
					},
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 0,
							},
						},
					},
				},
			},
		}
	)
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database, &MockBlockStorageHelper{})

	t.Run("Set and get block", func(t *testing.T) {
		_, err := storage.StoreBlock(ctx, newBlock)
		assert.NoError(t, err)

		block, err := storage.GetBlock(ctx, newBlock.BlockIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Get non-existent block", func(t *testing.T) {
		block, err := storage.GetBlock(ctx, badBlockIdentifier)
		assert.EqualError(
			t,
			err,
			fmt.Errorf("%w %+v", ErrBlockNotFound, badBlockIdentifier).Error(),
		)
		assert.Nil(t, block)
	})

	t.Run("Set duplicate block hash", func(t *testing.T) {
		_, err = storage.StoreBlock(ctx, newBlock)
		assert.EqualError(t, err, fmt.Errorf(
			"%w %s",
			ErrDuplicateBlockHash,
			newBlock.BlockIdentifier.Hash,
		).Error())
	})

	t.Run("Set duplicate transaction hash", func(t *testing.T) {
		_, err = storage.StoreBlock(ctx, newBlock2)
		assert.EqualError(t, err, fmt.Errorf(
			"%w %s",
			ErrDuplicateTransactionHash,
			"blahTx",
		).Error())
	})

	t.Run("Remove block and re-set block of same hash", func(t *testing.T) {
		_, err := storage.RemoveBlock(ctx, newBlock)
		assert.NoError(t, err)

		_, err = storage.StoreBlock(ctx, newBlock)
		assert.NoError(t, err)
	})
}

func TestGetAccountKey(t *testing.T) {
	var tests = map[string]struct {
		account *types.AccountIdentifier
		key     string
	}{
		"simple account": {
			account: &types.AccountIdentifier{
				Address: "hello",
			},
			key: "balance:hello",
		},
		"subaccount": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
				},
			},
			key: "balance:hello:stake",
		},
		"subaccount with string metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": "neat",
					},
				},
			},
			key: "balance:hello:stake:map[cool:neat]",
		},
		"subaccount with number metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": 1,
					},
				},
			},
			key: "balance:hello:stake:map[cool:1]",
		},
		"subaccount with complex metadata": {
			account: &types.AccountIdentifier{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool":    1,
						"awesome": "neat",
					},
				},
			},
			key: "balance:hello:stake:map[awesome:neat cool:1]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.key, GetAccountKey(test.account))
		})
	}
}

func TestBalance(t *testing.T) {
	var (
		account = &types.AccountIdentifier{
			Address: "blah",
		}
		account2 = &types.AccountIdentifier{
			Address: "blah2",
		}
		account3 = &types.AccountIdentifier{
			Address: "blah3",
		}
		subAccount = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
			},
		}
		subAccountNewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
			},
		}
		subAccountMetadata = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadataNewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadata2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": 10,
				},
			},
		}
		subAccountMetadata2NewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": 10,
				},
			},
		}
		currency = &types.Currency{
			Symbol:   "BLAH",
			Decimals: 2,
		}
		amount = &types.Amount{
			Value:    "100",
			Currency: currency,
		}
		amountWithPrevious = &types.Amount{
			Value:    "110",
			Currency: currency,
		}
		amountNilCurrency = &types.Amount{
			Value: "100",
		}
		newBlock = &types.BlockIdentifier{
			Hash:  "kdasdj",
			Index: 123890,
		}
		newBlock2 = &types.BlockIdentifier{
			Hash:  "pkdasdj",
			Index: 123890,
		}
		result = &types.Amount{
			Value:    "200",
			Currency: currency,
		}
		newBlock3 = &types.BlockIdentifier{
			Hash:  "pkdgdj",
			Index: 123891,
		}
		largeDeduction = &types.Amount{
			Value:    "-1000",
			Currency: currency,
		}
		mockHelper = &MockBlockStorageHelper{}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database, mockHelper)

	t.Run("Get unset balance", func(t *testing.T) {
		amount, block, err := storage.GetBalance(ctx, account, currency)
		assert.Nil(t, amount)
		assert.Nil(t, block)
		assert.EqualError(t, err, fmt.Errorf("%w %+v", ErrAccountNotFound, account).Error())
	})

	t.Run("Set and get balance", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Set and get balance with storage helper", func(t *testing.T) {
		mockHelper.AccountBalanceAmount = "10"
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account3,
				Currency:   currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, account3, currency)
		assert.NoError(t, err)
		assert.Equal(t, amountWithPrevious, retrievedAmount)
		assert.Equal(t, newBlock, block)

		mockHelper.AccountBalanceAmount = ""
	})

	t.Run("Set balance with nil currency", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account,
				Currency:   nil,
				Block:      newBlock,
				Difference: amountNilCurrency.Value,
			},
			nil,
		)
		assert.EqualError(t, err, "invalid currency")
		txn.Discard(ctx)

		retrievedAmount, block, err := storage.GetBalance(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Modify existing balance", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      newBlock2,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)
		assert.Equal(t, newBlock2, block)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      newBlock3,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)

		// Get balance during transaction
		retrievedAmount, block, err := storage.GetBalance(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)
		assert.Equal(t, newBlock2, block)

		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on existing account", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account,
				Currency:   largeDeduction.Currency,
				Block:      newBlock3,
				Difference: largeDeduction.Value,
			},
			nil,
		)
		assert.True(t, errors.Is(err, ErrNegativeBalance))
		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on new acct", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    account2,
				Currency:   largeDeduction.Currency,
				Block:      newBlock2,
				Difference: largeDeduction.Value,
			},
			nil,
		)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNegativeBalance))
		txn.Discard(ctx)
	})

	t.Run("sub account set and get balance", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    subAccount,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, subAccountNewPointer, amount.Currency)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, newBlock, block)
	})

	t.Run("sub account metadata set and get balance", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    subAccountMetadata,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, subAccountMetadataNewPointer, amount.Currency)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, newBlock, block)
	})

	t.Run("sub account unique metadata set and get balance", func(t *testing.T) {
		txn := storage.newDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
			ctx,
			txn,
			&reconciler.BalanceChange{
				Account:    subAccountMetadata2,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, block, err := storage.GetBalance(ctx, subAccountMetadata2NewPointer, amount.Currency)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, newBlock, block)
	})
}

func TestGetCurrencyKey(t *testing.T) {
	var tests = map[string]struct {
		currency *types.Currency
		key      string
	}{
		"simple currency": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			key: "BTC:8",
		},
		"currency with string metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
				},
			},
			key: "BTC:8:map[issuer:satoshi]",
		},
		"currency with number metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": 1,
				},
			},
			key: "BTC:8:map[issuer:1]",
		},
		"currency with complex metadata": {
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
					"count":  10,
				},
			},
			key: "BTC:8:map[count:10 issuer:satoshi]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.key, GetCurrencyKey(test.currency))
		})
	}
}

func TestBootstrapBalances(t *testing.T) {
	var (
		fileMode               = os.FileMode(0600)
		genesisBlockIdentifier = &types.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		}

		account = &types.AccountIdentifier{
			Address: "hello",
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database, &MockBlockStorageHelper{})
	bootstrapBalancesFile := path.Join(newDir, "balances.csv")

	t.Run("File doesn't exist", func(t *testing.T) {
		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, fmt.Sprintf(
			"open %s: no such file or directory",
			bootstrapBalancesFile,
		))
	})

	t.Run("Set balance successfully", func(t *testing.T) {
		amount := &types.Amount{
			Value: "10",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		file, err := json.MarshalIndent([]*BootstrapBalance{
			{
				Account:  account,
				Value:    amount.Value,
				Currency: amount.Currency,
			},
		}, "", " ")
		assert.NoError(t, err)

		assert.NoError(t, ioutil.WriteFile(bootstrapBalancesFile, file, fileMode))

		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.NoError(t, err)

		retrievedAmount, blockIdentifier, err := storage.GetBalance(
			ctx,
			account,
			amount.Currency,
		)

		assert.Equal(t, amount, retrievedAmount)
		assert.Equal(t, genesisBlockIdentifier, blockIdentifier)
		assert.NoError(t, err)
	})

	t.Run("Invalid file contents", func(t *testing.T) {
		assert.NoError(t, ioutil.WriteFile(bootstrapBalancesFile, []byte("bad file"), fileMode))

		err := storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.Error(t, err)
	})

	t.Run("Invalid account balance", func(t *testing.T) {
		amount := &types.Amount{
			Value: "-10",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		file, err := json.MarshalIndent([]*BootstrapBalance{
			{
				Account:  account,
				Value:    amount.Value,
				Currency: amount.Currency,
			},
		}, "", " ")
		assert.NoError(t, err)

		assert.NoError(t, ioutil.WriteFile(bootstrapBalancesFile, file, fileMode))

		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "cannot bootstrap zero or negative balance -10")
	})

	t.Run("Invalid account value", func(t *testing.T) {
		amount := &types.Amount{
			Value: "goodbye",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		file, err := json.MarshalIndent([]*BootstrapBalance{
			{
				Account:  account,
				Value:    amount.Value,
				Currency: amount.Currency,
			},
		}, "", " ")
		assert.NoError(t, err)

		assert.NoError(t, ioutil.WriteFile(bootstrapBalancesFile, file, fileMode))

		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "goodbye is not an integer")
	})

	t.Run("Head block identifier already set", func(t *testing.T) {
		tx := storage.newDatabaseTransaction(ctx, true)
		err := storage.StoreHeadBlockIdentifier(ctx, tx, genesisBlockIdentifier)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		// Use the created CSV file from the last test
		err = storage.BootstrapBalances(ctx, bootstrapBalancesFile, genesisBlockIdentifier)
		assert.EqualError(t, err, ErrAlreadyStartedSyncing.Error())
	})
}

type MockBlockStorageHelper struct {
	AccountBalanceAmount string
}

func (h *MockBlockStorageHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	value := "0"
	if len(h.AccountBalanceAmount) > 0 {
		value = h.AccountBalanceAmount
	}

	return &types.Amount{
		Value:    value,
		Currency: currency,
	}, nil
}

func (h *MockBlockStorageHelper) SkipOperation(
	ctx context.Context,
	op *types.Operation,
) (bool, error) {
	if op.Account == nil || op.Amount == nil {
		return true, nil
	}

	return false, nil
}
