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
	"os"
	"path"
	"testing"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"

	"github.com/stretchr/testify/assert"
)

func TestHeadBlockIdentifier(t *testing.T) {
	var (
		newBlockIdentifier = &rosetta.BlockIdentifier{
			Hash:  "blah",
			Index: 0,
		}
		newBlockIdentifier2 = &rosetta.BlockIdentifier{
			Hash:  "blah2",
			Index: 1,
		}
	)

	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database)

	t.Run("No head block set", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, false)
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx, txn)
		txn.Discard(ctx)
		assert.EqualError(t, err, ErrHeadBlockNotFound.Error())
		assert.Nil(t, blockIdentifier)
	})

	t.Run("Set and get head block", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier))
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx, txn)
		assert.NoError(t, err)
		txn.Discard(ctx)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Discard head block update", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn,
			&rosetta.BlockIdentifier{
				Hash:  "no blah",
				Index: 10,
			}),
		)
		txn.Discard(ctx)

		txn = storage.NewDatabaseTransaction(ctx, false)
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx, txn)
		assert.NoError(t, err)
		txn.Discard(ctx)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Multiple updates to head block", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier2))
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx, txn)
		assert.NoError(t, err)
		txn.Discard(ctx)
		assert.Equal(t, newBlockIdentifier2, blockIdentifier)
	})
}

func TestBlock(t *testing.T) {
	var (
		newBlock = &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Hash:  "blah",
				Index: 0,
			},
			Timestamp: 1,
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "blahTx",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
						},
					},
				},
			},
		}

		badBlockIdentifier = &rosetta.BlockIdentifier{
			Hash:  "missing blah",
			Index: 0,
		}

		newBlock2 = &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Hash:  "blah 2",
				Index: 1,
			},
			Timestamp: 1,
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "blahTx",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index: 0,
							},
						},
					},
				},
			},
		}
	)
	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database)

	t.Run("Set and get block", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreBlock(ctx, txn, newBlock))
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		block, err := storage.GetBlock(ctx, txn, newBlock.BlockIdentifier)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Get non-existent block", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, false)
		block, err := storage.GetBlock(ctx, txn, badBlockIdentifier)
		txn.Discard(ctx)
		assert.EqualError(
			t,
			err,
			fmt.Errorf("%w %+v", ErrBlockNotFound, badBlockIdentifier).Error(),
		)
		assert.Nil(t, block)
	})

	t.Run("Set duplicate block hash", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		err = storage.StoreBlock(ctx, txn, newBlock)
		assert.EqualError(t, err, fmt.Errorf(
			"%w %s",
			ErrDuplicateBlockHash,
			newBlock.BlockIdentifier.Hash,
		).Error())
		txn.Discard(ctx)
	})

	t.Run("Set duplicate transaction hash", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		err = storage.StoreBlock(ctx, txn, newBlock2)
		assert.EqualError(t, err, fmt.Errorf(
			"%w %s",
			ErrDuplicateTransactionHash,
			"blahTx",
		).Error())
		txn.Discard(ctx)
	})

	t.Run("Remove block and re-set block of same hash", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.RemoveBlock(ctx, txn, newBlock.BlockIdentifier))
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, true)
		assert.NoError(t, storage.StoreBlock(ctx, txn, newBlock))
		assert.NoError(t, txn.Commit(ctx))
	})
}

func TestGetBalanceKey(t *testing.T) {
	var tests = map[string]struct {
		account *rosetta.AccountIdentifier
		key     string
	}{
		"simple account": {
			account: &rosetta.AccountIdentifier{
				Address: "hello",
			},
			key: "balance:hello",
		},
		"subaccount": {
			account: &rosetta.AccountIdentifier{
				Address: "hello",
				SubAccount: &rosetta.SubAccountIdentifier{
					SubAccount: "stake",
				},
			},
			key: "balance:hello:stake",
		},
		"subaccount with string metadata": {
			account: &rosetta.AccountIdentifier{
				Address: "hello",
				SubAccount: &rosetta.SubAccountIdentifier{
					SubAccount: "stake",
					Metadata: &map[string]interface{}{
						"cool": "neat",
					},
				},
			},
			key: "balance:hello:stake:map[cool:neat]",
		},
		"subaccount with number metadata": {
			account: &rosetta.AccountIdentifier{
				Address: "hello",
				SubAccount: &rosetta.SubAccountIdentifier{
					SubAccount: "stake",
					Metadata: &map[string]interface{}{
						"cool": 1,
					},
				},
			},
			key: "balance:hello:stake:map[cool:1]",
		},
		"subaccount with complex metadata": {
			account: &rosetta.AccountIdentifier{
				Address: "hello",
				SubAccount: &rosetta.SubAccountIdentifier{
					SubAccount: "stake",
					Metadata: &map[string]interface{}{
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
			assert.Equal(t, hashBytes([]byte(test.key)), getBalanceKey(test.account))
		})
	}
}

func TestBalance(t *testing.T) {
	var (
		account = &rosetta.AccountIdentifier{
			Address: "blah",
		}
		account2 = &rosetta.AccountIdentifier{
			Address: "blah2",
		}
		subAccount = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
			},
		}
		subAccountNewPointer = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
			},
		}
		subAccountMetadata = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
				Metadata: &map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadataNewPointer = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
				Metadata: &map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadata2 = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
				Metadata: &map[string]interface{}{
					"cool": 10,
				},
			},
		}
		subAccountMetadata2NewPointer = &rosetta.AccountIdentifier{
			Address: "blah",
			SubAccount: &rosetta.SubAccountIdentifier{
				SubAccount: "stake",
				Metadata: &map[string]interface{}{
					"cool": 10,
				},
			},
		}
		currency = &rosetta.Currency{
			Symbol:   "BLAH",
			Decimals: 2,
		}
		amount = &rosetta.Amount{
			Value:    "100",
			Currency: currency,
		}
		amountNilCurrency = &rosetta.Amount{
			Value: "100",
		}
		newAmounts = map[string]*rosetta.Amount{
			GetCurrencyKey(currency): amount,
		}
		newBlock = &rosetta.BlockIdentifier{
			Hash:  "kdasdj",
			Index: 123890,
		}
		newTransaction = &rosetta.TransactionIdentifier{
			Hash: "tx1",
		}
		newBlock2 = &rosetta.BlockIdentifier{
			Hash:  "pkdasdj",
			Index: 123890,
		}
		newTransaction2 = &rosetta.TransactionIdentifier{
			Hash: "tx2",
		}
		result = map[string]*rosetta.Amount{
			GetCurrencyKey(currency): {
				Value:    "200",
				Currency: currency,
			},
		}
		newBlock3 = &rosetta.BlockIdentifier{
			Hash:  "pkdgdj",
			Index: 123891,
		}
		largeDeduction = &rosetta.Amount{
			Value:    "-1000",
			Currency: currency,
		}
	)

	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database)

	t.Run("Get unset balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, account)
		txn.Discard(ctx)
		assert.Nil(t, amounts)
		assert.Nil(t, block)
		assert.EqualError(t, err, fmt.Errorf("%w %+v", ErrAccountNotFound, account).Error())
	})

	t.Run("Set and get balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account,
			amount,
			newBlock,
			newTransaction,
		)
		assert.Equal(t, &BalanceChange{
			Account: &rosetta.AccountIdentifier{
				Address: "blah",
			},
			Currency:    currency,
			Block:       newBlock,
			Transaction: newTransaction,
			OldValue:    "0",
			NewValue:    "100",
			Difference:  "100",
		}, balanceChange)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, account)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newAmounts, amounts)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Set balance with nil currency", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account,
			amountNilCurrency,
			newBlock,
			nil,
		)
		assert.Nil(t, balanceChange)
		assert.EqualError(t, err, "invalid amount")
		txn.Discard(ctx)

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, account)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newAmounts, amounts)
		assert.Equal(t, newBlock, block)
	})

	t.Run("Modify existing balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account,
			amount,
			newBlock2,
			newTransaction2,
		)
		assert.Equal(t, &BalanceChange{
			Account: &rosetta.AccountIdentifier{
				Address: "blah",
			},
			Currency:    currency,
			Block:       newBlock2,
			OldBlock:    newBlock,
			Transaction: newTransaction2,
			OldValue:    "100",
			NewValue:    "200",
			Difference:  "100",
		}, balanceChange)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, true)
		amounts, block, err := storage.GetBalance(ctx, txn, account)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, result, amounts)
		assert.Equal(t, newBlock2, block)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account,
			amount,
			newBlock3,
			nil,
		)
		assert.Equal(t, &BalanceChange{
			Account: &rosetta.AccountIdentifier{
				Address: "blah",
			},
			Currency:   currency,
			Block:      newBlock3,
			OldBlock:   newBlock2,
			OldValue:   "200",
			NewValue:   "300",
			Difference: "100",
		}, balanceChange)
		assert.NoError(t, err)

		// Get balance during transaction
		txn2 := storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn2, account)
		txn2.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, result, amounts)
		assert.Equal(t, newBlock2, block)

		txn.Discard(ctx)

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err = storage.GetBalance(ctx, txn, account)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, result, amounts)
		assert.Equal(t, newBlock2, block)
	})

	t.Run("Attempt modification to push balance negative on existing account", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account,
			largeDeduction,
			newBlock2,
			nil,
		)
		assert.Nil(t, balanceChange)
		assert.Contains(t, err.Error(), ErrNegativeBalance.Error())
		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on new acct", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			account2,
			largeDeduction,
			newBlock2,
			nil,
		)
		assert.Nil(t, balanceChange)
		assert.Contains(t, err.Error(), ErrNegativeBalance.Error())
		txn.Discard(ctx)
	})

	t.Run("sub account set and get balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			subAccount,
			amount,
			newBlock,
			newTransaction,
		)
		assert.Equal(t, &BalanceChange{
			Account:     subAccount,
			Currency:    currency,
			Block:       newBlock,
			Transaction: newTransaction,
			OldValue:    "0",
			NewValue:    "100",
			Difference:  "100",
		}, balanceChange)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, subAccountNewPointer)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newAmounts, amounts)
		assert.Equal(t, newBlock, block)
	})

	t.Run("sub account metadata set and get balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			subAccountMetadata,
			amount,
			newBlock,
			newTransaction,
		)
		assert.Equal(t, &BalanceChange{
			Account:     subAccountMetadata,
			Currency:    currency,
			Block:       newBlock,
			Transaction: newTransaction,
			OldValue:    "0",
			NewValue:    "100",
			Difference:  "100",
		}, balanceChange)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, subAccountMetadataNewPointer)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newAmounts, amounts)
		assert.Equal(t, newBlock, block)
	})

	t.Run("sub account unique metadata set and get balance", func(t *testing.T) {
		txn := storage.NewDatabaseTransaction(ctx, true)
		balanceChange, err := storage.UpdateBalance(
			ctx,
			txn,
			subAccountMetadata2,
			amount,
			newBlock,
			newTransaction,
		)
		assert.Equal(t, &BalanceChange{
			Account:     subAccountMetadata2,
			Currency:    currency,
			Block:       newBlock,
			Transaction: newTransaction,
			OldValue:    "0",
			NewValue:    "100",
			Difference:  "100",
		}, balanceChange)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		txn = storage.NewDatabaseTransaction(ctx, false)
		amounts, block, err := storage.GetBalance(ctx, txn, subAccountMetadata2NewPointer)
		txn.Discard(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newAmounts, amounts)
		assert.Equal(t, newBlock, block)
	})
}

func TestGetCurrencyKey(t *testing.T) {
	var tests = map[string]struct {
		currency *rosetta.Currency
		key      string
	}{
		"simple currency": {
			currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			key: "BTC:8",
		},
		"currency with string metadata": {
			currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: &map[string]interface{}{
					"issuer": "satoshi",
				},
			},
			key: "BTC:8:map[issuer:satoshi]",
		},
		"currency with number metadata": {
			currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: &map[string]interface{}{
					"issuer": 1,
				},
			},
			key: "BTC:8:map[issuer:1]",
		},
		"currency with complex metadata": {
			currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: &map[string]interface{}{
					"issuer": "satoshi",
					"count":  10,
				},
			},
			key: "BTC:8:map[count:10 issuer:satoshi]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, hashString(test.key), GetCurrencyKey(test.currency))
		})
	}
}

func createBootstrapBalancesFile(dataDir string) (*os.File, error) {
	return os.OpenFile(
		path.Join(dataDir, bootstrapBalancesFile),
		os.O_CREATE|os.O_WRONLY,
		0600,
	)
}

func TestBootstrapBalances(t *testing.T) {
	var (
		genesisBlockIdentifier = &rosetta.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		}

		account = &rosetta.AccountIdentifier{
			Address: "hello",
		}
	)

	ctx := context.Background()

	newDir, err := CreateTempDir()
	assert.NoError(t, err)
	defer RemoveTempDir(*newDir)

	database, err := NewBadgerStorage(ctx, *newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(ctx, database)

	t.Run("File doesn't exist", func(t *testing.T) {
		err = storage.BootstrapBalances(
			ctx,
			*newDir,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, fmt.Sprintf(
			"open %s: no such file or directory",
			path.Join(*newDir, bootstrapBalancesFile),
		))
	})

	t.Run("Set balance successfully", func(t *testing.T) {
		f, err := createBootstrapBalancesFile(*newDir)
		assert.NoError(t, err)
		defer f.Close()

		_, err = f.WriteString(fmt.Sprintf(
			"%s\n",
			bootstrapBalancesHeader,
		))
		assert.NoError(t, err)

		amount := &rosetta.Amount{
			Value: "10",
			Currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		_, err = f.WriteString(fmt.Sprintf(
			"%s,%s,%s,%d\n",
			account.Address,
			amount.Value,
			amount.Currency.Symbol,
			amount.Currency.Decimals,
		))
		assert.NoError(t, err)

		err = storage.BootstrapBalances(
			ctx,
			*newDir,
			genesisBlockIdentifier,
		)
		assert.NoError(t, err)

		tx := storage.NewDatabaseTransaction(ctx, false)
		amountMap, blockIdentifier, err := storage.GetBalance(
			ctx,
			tx,
			account,
		)
		tx.Discard(ctx)

		assert.Equal(t, amount, amountMap[GetCurrencyKey(amount.Currency)])
		assert.Equal(t, genesisBlockIdentifier, blockIdentifier)
		assert.NoError(t, err)
	})

	t.Run("Invalid file header", func(t *testing.T) {
		f, err := createBootstrapBalancesFile(*newDir)
		assert.NoError(t, err)
		defer f.Close()

		_, err = f.WriteString("bad header")
		assert.NoError(t, err)

		err = storage.BootstrapBalances(
			ctx,
			*newDir,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, ErrIncorrectHeader.Error())
	})

	t.Run("Invalid account row", func(t *testing.T) {
		f, err := createBootstrapBalancesFile(*newDir)
		assert.NoError(t, err)
		defer f.Close()

		_, err = f.WriteString(fmt.Sprintf(
			"%s\n",
			bootstrapBalancesHeader,
		))
		assert.NoError(t, err)

		_, err = f.WriteString("bad row\n")
		assert.NoError(t, err)

		err = storage.BootstrapBalances(
			ctx,
			*newDir,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "row 2 does not have expected fields: [bad row]")
	})

	t.Run("Invalid account value", func(t *testing.T) {
		f, err := createBootstrapBalancesFile(*newDir)
		assert.NoError(t, err)
		defer f.Close()

		_, err = f.WriteString(fmt.Sprintf(
			"%s\n",
			bootstrapBalancesHeader,
		))
		assert.NoError(t, err)

		amount := &rosetta.Amount{
			Value: "goodbye",
			Currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		_, err = f.WriteString(fmt.Sprintf(
			"%s,%s,%s,%d\n",
			account.Address,
			amount.Value,
			amount.Currency.Symbol,
			amount.Currency.Decimals,
		))
		assert.NoError(t, err)

		err = storage.BootstrapBalances(
			ctx,
			*newDir,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "goodbye is not an integer")
	})

	t.Run("Head block identifier already set", func(t *testing.T) {
		tx := storage.NewDatabaseTransaction(ctx, true)
		err := storage.StoreHeadBlockIdentifier(ctx, tx, genesisBlockIdentifier)
		assert.NoError(t, err)
		assert.NoError(t, tx.Commit(ctx))

		// Use the created CSV file from the last test
		err = storage.BootstrapBalances(ctx, *newDir, genesisBlockIdentifier)
		assert.EqualError(t, err, ErrAlreadyStartedSyncing.Error())
	})
}
