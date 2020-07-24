package storage

import (
	"errors"
	"fmt"
)

const (
	// blockHashNamespace is prepended to any stored block hash.
	// We cannot just use the stored block key to lookup whether
	// a hash has been used before because it is concatenated
	// with the index of the stored block.
	blockHashNamespace = "block-hash"

	// transactionHashNamespace is prepended to any stored
	// transaction hash.
	transactionHashNamespace = "transaction-hash"
)

var (
	// ErrDuplicateBlockHash is returned when a block hash
	// cannot be stored because it is a duplicate.
	ErrDuplicateBlockHash = errors.New("duplicate block hash")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("duplicate transaction hash")

	// ErrDuplicateKey is returned when trying to store a key that
	// already exists (this is used by storeHash).
	ErrDuplicateKey = errors.New("duplicate key")
)

/*
  Key Construction
*/

func getBlockHashKey(hash string) []byte {
	return []byte(fmt.Sprintf("%s/%s", blockHashNamespace, hash))
}

func getTransactionHashKey(blockHash string, transactionHash string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", transactionHashNamespace, blockHash, transactionHash))
}

// HashStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type HashStorage struct {
	db Database
}

// NewHashStorage returns a new HashStorage.
func NewHashStorage(
	db Database,
) *HashStorage {
	return &HashStorage{
		db: db,
	}
}

// storeHash stores either a block or transaction hash.
func (b *BlockStorage) storeHash(
	ctx context.Context,
	transaction DatabaseTransaction,
	hashKey []byte,
) error {
	exists, _, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	if exists {
		return ErrDuplicateKey
	}

	return transaction.Set(ctx, hashKey, []byte(""))
}

func (h *HashStorage) StoreBlockHashes() {
	// Store block hash
	blockHashKey := getBlockHashKey(block.BlockIdentifier.Hash)
	err = b.storeHash(ctx, transaction, blockHashKey)
	if errors.Is(err, ErrDuplicateKey) {
		return nil, fmt.Errorf(
			"%w %s",
			ErrDuplicateBlockHash,
			block.BlockIdentifier.Hash,
		)
	} else if err != nil {
		return nil, fmt.Errorf("%w: unable to store block hash", err)
	}

	// Store all transaction hashes
	for _, txn := range block.Transactions {
		transactionHashKey := getTransactionHashKey(
			block.BlockIdentifier.Hash,
			txn.TransactionIdentifier.Hash,
		)
		err = b.storeHash(ctx, transaction, transactionHashKey)
		if errors.Is(err, ErrDuplicateKey) {
			return nil, fmt.Errorf(
				"%w transaction %s appears multiple times in block %s",
				ErrDuplicateTransactionHash,
				txn.TransactionIdentifier.Hash,
				block.BlockIdentifier.Hash,
			)
		} else if err != nil {
			return nil, fmt.Errorf("%w: unable to store transaction hash", err)
		}
	}
}

func RemoveBlockHashes() {
	// Remove all transaction hashes
	for _, txn := range block.Transactions {
		err = transaction.Delete(
			ctx,
			getTransactionHashKey(block.BlockIdentifier.Hash, txn.TransactionIdentifier.Hash),
		)
		if err != nil {
			return nil, err
		}
	}

	// Remove block hash
	err = transaction.Delete(ctx, getBlockHashKey(block.BlockIdentifier.Hash))
	if err != nil {
		return nil, err
	}

}
