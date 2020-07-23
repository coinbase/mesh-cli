package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"

	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	UtxoCreated = "utxo_created"
	UtxoSpent   = "utxo_spent"
)

type CheckConstructionHandler struct {
	// TODO: use badger storage
	txStore map[string]*types.Block

	utxoStore       map[string][]*UTXO
	utxoEnabled     bool
	lastBlockSynced int64 // keep syncing across restarts

	syncMutex sync.Mutex
}

func NewCheckConstructionHandler(
	ctx context.Context,
	trackUTXOs bool,
) *CheckConstructionHandler {
	handler := &CheckConstructionHandler{
		txStore:         make(map[string]*types.Block),
		lastBlockSynced: -1,
	}

	if trackUTXOs {
		handler.utxoStore = make(map[string][]*UTXO)
		handler.utxoEnabled = true
		log.Println("initialized UTXO store")
	}

	return handler
}

func (t *CheckConstructionHandler) extractUTXOs(
	ctx context.Context,
	transaction *types.Transaction,
) error {
	for _, operation := range transaction.Operations {
		utxoCreated, ok := operation.Metadata[UtxoCreated]
		if ok {
			id, ok := utxoCreated.(string)
			if !ok {
				return fmt.Errorf("unable to parse created utxo %v", utxoCreated)
			}

			newUTXO := &UTXO{
				Identifier:  id,
				Transaction: transaction,
				Operation:   operation,
			}

			t.utxoStore[operation.Account.Address] = append(t.utxoStore[operation.Account.Address], newUTXO)
			continue // operation should not spend and create utxo
		}

		utxoSpent, ok := operation.Metadata[UtxoSpent]
		if ok {
			id, ok := utxoSpent.(string)
			if !ok {
				return fmt.Errorf("unable to parse spent utxo %v", utxoSpent)
			}

			removedIndex := -1
			for i, ut := range t.utxoStore[operation.Account.Address] {
				if ut.Identifier != id {
					continue
				}

				removedIndex = i
				break
			}

			if removedIndex != -1 { // if -1 then from before tracking and should do nothing
				t.utxoStore[operation.Account.Address] = append(
					t.utxoStore[operation.Account.Address][:removedIndex],
					t.utxoStore[operation.Account.Address][removedIndex+1:]...,
				)
			}
		}
	}

	return nil
}

func (t *CheckConstructionHandler) BlockAdded(
	ctx context.Context,
	block *types.Block,
) error {
	t.syncMutex.Lock()
	for _, tx := range block.Transactions {
		// Store all transactions seen
		t.txStore[tx.TransactionIdentifier.Hash] = block

		// Store all coins
		if err := t.extractUTXOs(ctx, tx); err != nil {
			t.syncMutex.Unlock()
			return fmt.Errorf("%w: failed to extract utxos", err)
		}
	}
	t.lastBlockSynced = block.BlockIdentifier.Index
	t.syncMutex.Unlock()

	return nil
}

func (t *CheckConstructionHandler) BlockRemoved(
	ctx context.Context,
	block *types.BlockIdentifier,
) error {
	// TODO: handle re-orgs
	return nil
}

func (t *CheckConstructionHandler) Transaction(
	ctx context.Context,
	transaction *types.TransactionIdentifier,
) (*types.BlockIdentifier, *types.Transaction) {
	block, ok := t.txStore[transaction.Hash]
	if !ok {
		return nil, nil
	}

	var matchingTx *types.Transaction
	for _, txn := range block.Transactions {
		if types.Hash(transaction) == types.Hash(txn.TransactionIdentifier) {
			matchingTx = txn
			break
		}
	}

	return block.BlockIdentifier, matchingTx
}

type UTXO struct {
	Identifier  string             `json:"identifier"` // uses "utxo_created" or "utxo_spent"
	Transaction *types.Transaction `json:"tx"`
	Operation   *types.Operation   `json:"op"`
}

func (t *CheckConstructionHandler) GetUTXOBalance(address string) (*big.Int, []*UTXO, int64, error) {
	if t == nil { // when first starting, there is a race
		return big.NewInt(0), []*UTXO{}, -1, nil
	}

	if !t.utxoEnabled {
		return nil, nil, -1, errors.New("utxo tracking not enabled")
	}

	t.syncMutex.Lock()
	lastIndex := t.lastBlockSynced

	val, exists := t.utxoStore[address]
	if !exists {
		t.syncMutex.Unlock()
		return big.NewInt(0), []*UTXO{}, lastIndex, nil
	}

	amount := big.NewInt(0)
	validUTXOS := []*UTXO{}
	for _, v := range val {
		opAmount, ok := new(big.Int).SetString(v.Operation.Amount.Value, 10)
		if !ok {
			return nil, nil, -1, errors.New("utxo corruption detected")
		}

		amount = new(big.Int).Add(amount, opAmount)
		validUTXOS = append(validUTXOS, v)
	}

	t.syncMutex.Unlock()
	return amount, validUTXOS, lastIndex, nil
}
