package processor

import (
	"context"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Processor is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync processor.

// TODO: move back to sync handler...create struct that is SyncHandler and Reconciler Handler
type Processor interface {
	// TODO: if account appears for the first time, sync its balance at block previous if
	// lookup by balance enabled. If not enabled, then warn that must sync from genesis...then
	// we can use same logic for all use cases. :fire:
	BlockAdded(
		ctx context.Context,
		block *types.Block,
	) error

	BlockRemoved(
		ctx context.Context,
		block *types.Block,
	) error
}
