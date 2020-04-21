package syncer

import (
	"context"
	"testing"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func simpleTransactionFactory(
	hash string,
	address string,
	value string,
	currency *types.Currency,
) *types.Transaction {
	return &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: hash,
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 0,
				},
				Type:   "Transfer",
				Status: "Success",
				Account: &types.AccountIdentifier{
					Address: address,
				},
				Amount: &types.Amount{
					Value:    value,
					Currency: currency,
				},
			},
		},
	}

}

func TestBalanceChanges(t *testing.T) {
	var tests = map[string]struct {
		block   *types.Block
		orphan  bool
		changes []*storage.BalanceChange
		err     error
	}{
		"simple block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					recipientTransaction,
				},
			},
			orphan: false,
			changes: []*storage.BalanceChange{
				{
					Account:  recipient,
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "100",
				},
			},
			err: nil,
		},
		"single account sum block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
			},
			orphan: false,
			changes: []*storage.BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "150",
				},
			},
			err: nil,
		},
		"single account sum orphan block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
			},
			orphan: true,
			changes: []*storage.BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "0",
						Index: 0,
					},
					Difference: "-250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "0",
						Index: 0,
					},
					Difference: "-150",
				},
			},
			err: nil,
		},
	}

	ctx := context.Background()
	asserter, err := asserter.NewWithResponses(
		ctx,
		networkStatusResponse,
		networkOptionsResponse,
	)
	assert.NotNil(t, asserter)
	assert.NoError(t, err)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			changes, err := BalanceChanges(
				ctx,
				asserter,
				test.block,
				test.orphan,
			)

			assert.Equal(t, test.changes, changes)
			assert.Equal(t, test.err, err)
		})
	}
}
