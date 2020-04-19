package syncer

import (
	"context"

	"github.com/coinbase/rosetta-validator/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Handler is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync handler.
type Handler interface {
	BlockProcessed(
		context.Context,
		*types.Block,
		bool,
		[]*storage.BalanceChange,
	) error
}
