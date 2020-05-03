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
	"fmt"
	"log"
	"math/big"

	"github.com/coinbase/rosetta-cli/internal/logger"
	"github.com/coinbase/rosetta-cli/internal/reconciler"
	"github.com/coinbase/rosetta-cli/internal/storage"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// BaseProcessor logs processed blocks
// and reconciles modified balances.
type BaseProcessor struct {
	logger         *logger.Logger
	reconciler     reconciler.Reconciler
	asserter       *asserter.Asserter
	exemptAccounts []*reconciler.AccountCurrency
}

// NewBaseProcessor constructs a basic Handler.
func NewBaseProcessor(
	logger *logger.Logger,
	reconciler reconciler.Reconciler,
	asserter *asserter.Asserter,
	exemptAccounts []*reconciler.AccountCurrency,
) *BaseProcessor {
	return &BaseProcessor{
		logger:         logger,
		reconciler:     reconciler,
		asserter:       asserter,
		exemptAccounts: exemptAccounts,
	}
}

// BalanceChanges returns all balance changes for
// a particular block. All balance changes for a
// particular account are summed into a single
// storage.BalanceChanges struct. If a block is being
// orphaned, the opposite of each balance change is
// returned.
func (p *BaseProcessor) BalanceChanges(
	ctx context.Context,
	block *types.Block,
	blockRemoved bool,
) ([]*storage.BalanceChange, error) {
	balanceChanges := map[string]*storage.BalanceChange{}
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			skip, err := p.skipOperation(
				ctx,
				op,
			)
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}

			amount := op.Amount
			blockIdentifier := block.BlockIdentifier
			if blockRemoved {
				existing, ok := new(big.Int).SetString(amount.Value, 10)
				if !ok {
					return nil, fmt.Errorf("%s is not an integer", amount.Value)
				}

				amount.Value = new(big.Int).Neg(existing).String()
				blockIdentifier = block.ParentBlockIdentifier
			}

			// Merge values by account and currency
			// TODO: change balance key to be this
			key := fmt.Sprintf("%s:%s",
				storage.GetAccountKey(op.Account),
				storage.GetCurrencyKey(op.Amount.Currency),
			)

			val, ok := balanceChanges[key]
			if !ok {
				balanceChanges[key] = &storage.BalanceChange{
					Account:    op.Account,
					Currency:   op.Amount.Currency,
					Difference: amount.Value,
					Block:      blockIdentifier,
				}
				continue
			}

			newDifference, err := storage.AddStringValues(val.Difference, amount.Value)
			if err != nil {
				return nil, err
			}
			val.Difference = newDifference
			balanceChanges[key] = val
		}
	}

	allChanges := []*storage.BalanceChange{}
	for _, change := range balanceChanges {
		allChanges = append(allChanges, change)
	}

	return allChanges, nil
}

// skipOperation returns a boolean indicating whether
// an operation should be processed. An operation will
// not be processed if it is considered unsuccessful
// or affects an exempt account.
func (p *BaseProcessor) skipOperation(
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
func (p *BaseProcessor) BlockAdded(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Adding block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := p.logger.BlockStream(ctx, block, false); err != nil {
		return nil
	}

	balanceChanges, err := p.BalanceChanges(ctx, block, false)
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
func (p *BaseProcessor) BlockRemoved(
	ctx context.Context,
	block *types.Block,
) error {
	log.Printf("Orphaning block %+v\n", block.BlockIdentifier)

	// Log processed blocks and balance changes
	if err := p.logger.BlockStream(ctx, block, true); err != nil {
		return nil
	}

	balanceChanges, err := p.BalanceChanges(ctx, block, true)
	if err != nil {
		return err
	}

	if err := p.logger.BalanceStream(ctx, balanceChanges); err != nil {
		return nil
	}

	return nil
}

// accountExempt returns a boolean indicating if the provided
// account and currency are exempt from balance tracking and
// reconciliation.
func (p *BaseProcessor) accountExempt(
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
