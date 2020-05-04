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

package processor

import (
	"context"
	"log"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Processor is what you write to handle chain data.
type Processor struct {
	storage        *storage.BlockStorage
	logger         *logger.Logger
	reconciler     *reconciler.Reconciler
	asserter       *asserter.Asserter
	exemptAccounts []*reconciler.AccountCurrency
}

// NewProcessor constructs a basic Handler.
func NewProcessor(
	storage *storage.BlockStorage,
	logger *logger.Logger,
	reconciler *reconciler.Reconciler,
	asserter *asserter.Asserter,
	exemptAccounts []*reconciler.AccountCurrency,
) *Processor {
	return &Processor{
		storage:        storage,
		logger:         logger,
		reconciler:     reconciler,
		asserter:       asserter,
		exemptAccounts: exemptAccounts,
	}
}

// SkipOperation returns a boolean indicating whether
// an operation should be processed. An operation will
// not be processed if it is considered unsuccessful
// or affects an exempt account.
func (p *Processor) SkipOperation(
	ctx context.Context,
	op *types.Operation,
) (bool, error) {
	successful, err := p.asserter.OperationSuccessful(op)
	if err != nil {
		// Should only occur if responses not validated
		return false, err
	}

	if !successful {
		return true, nil
	}

	if op.Account == nil {
		return true, nil
	}

	// Exempting account in BalanceChanges ensures that storage is not updated
	// and that the account is not reconciled.
	if p.accountExempt(ctx, op.Account, op.Amount.Currency) {
		log.Printf("Skipping exempt account %+v\n", op.Account)
		return true, nil
	}

	return false, nil
}

// BlockAdded is called by the syncer after a
// block is added.
func (p *Processor) BlockAdded(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Adding block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := p.logger.BlockStream(ctx, block, false); err != nil {
		return nil
	}

	balanceChanges, err := p.storage.StoreBlock(ctx, block)
	if err != nil {
		return err
	}

	if err := p.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// Mark accounts for reconciliation...this may be
	// blocking
	return p.reconciler.QueueChanges(ctx, block.BlockIdentifier, balanceChanges)
}

// BlockRemoved is called by the syncer after a
// block is removed.
func (p *Processor) BlockRemoved(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Orphaning block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := p.logger.BlockStream(ctx, block, true); err != nil {
		return nil
	}

	balanceChanges, err := p.storage.RemoveBlock(ctx, block)
	if err != nil {
		return err
	}

	if err := p.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	// We only attempt to reconciler changes when blocks are added
	return nil
}

// accountExempt returns a boolean indicating if the provided
// account and currency are exempt from balance tracking and
// reconciliation.
func (p *Processor) accountExempt(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
) bool {
	return reconciler.ContainsAccountCurrency(
		p.exemptAccounts,
		&reconciler.AccountCurrency{
			Account:  account,
			Currency: currency,
		},
	)
}
