package processor

import (
	"context"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Processor is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync processor.
type Processor interface {
	BlockAdded(
		ctx context.Context,
		block *types.Block,
	) error

	BlockRemoved(
		ctx context.Context,
		block *types.Block,
	) error
}
