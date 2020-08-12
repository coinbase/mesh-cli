package tester

import (
	"context"
	"fmt"
	"math/big"
	"path"
	"testing"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/storage"
	"github.com/coinbase/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var (
	tr = true
	f  = false
)

func TestComputeCheckDataResults(t *testing.T) {
	var tests = map[string]struct {
		cfg *configuration.Configuration

		// counter storage values
		provideCounterStorage   bool
		blockCount              int64
		operationCount          int64
		activeReconciliations   int64
		inactiveReconciliations int64

		// balance storage values
		provideBalanceStorage bool
		totalAccounts         int
		reconciledAccounts    int

		// We use a slice of errors here because
		// there typically a collection of errors
		// that should return the same result.
		err []error

		result *CheckDataResults
	}{
		"default configuration, no storage, no error": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{nil},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
				},
			},
		},
		"default configuration, no storage, fetch errors": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{fetcher.ErrExhaustedRetries, fetcher.ErrRequestFailed, fetcher.ErrNoNetworks, utils.ErrNetworkNotSupported},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   false,
					ResponseAssertion: true,
				},
			},
		},
		"default configuration, no storage, assertion errors": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{fetcher.ErrAssertionFailed},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: false,
				},
			},
		},
		"default configuration, no storage, syncing errors": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{syncer.ErrCannotRemoveGenesisBlock, syncer.ErrOutOfOrder, storage.ErrDuplicateKey, storage.ErrDuplicateTransactionHash},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
					BlockSyncing:      &f,
				},
			},
		},
		"default configuration, counter storage no blocks, balance errors": {
			cfg:                   configuration.DefaultConfiguration(),
			provideCounterStorage: true,
			err:                   []error{storage.ErrNegativeBalance},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
					BalanceTracking:   &f,
				},
				Stats: &CheckDataStats{},
			},
		},
		"default configuration, counter storage with blocks, balance errors": {
			cfg:                   configuration.DefaultConfiguration(),
			provideCounterStorage: true,
			blockCount:            100,
			err:                   []error{storage.ErrNegativeBalance},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
					BlockSyncing:      &tr,
					BalanceTracking:   &f,
				},
				Stats: &CheckDataStats{
					Blocks: 100,
				},
			},
		},
		"default configuration, no storage, balance errors": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{storage.ErrNegativeBalance},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
					BalanceTracking:   &f,
				},
			},
		},
		"default configuration, no storage, reconciliation errors": {
			cfg: configuration.DefaultConfiguration(),
			err: []error{processor.ErrReconciliationFailure},
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					RequestResponse:   true,
					ResponseAssertion: true,
					Reconciliation:    &f,
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for _, err := range test.err {
				testName := "nil"
				var testErr error
				if err != nil {
					testName = err.Error()
					testErr = fmt.Errorf("%w: test wrapping", err)
					test.result.Error = testErr.Error()
				}

				dir, err := utils.CreateTempDir()
				assert.NoError(t, err)

				ctx := context.Background()
				localStore, err := storage.NewBadgerStorage(ctx, dir, false)
				assert.NoError(t, err)

				logPath := path.Join(dir, "results.json")

				var counterStorage *storage.CounterStorage
				if test.provideCounterStorage {
					counterStorage = storage.NewCounterStorage(localStore)
					_, err = counterStorage.Update(ctx, storage.BlockCounter, big.NewInt(test.blockCount))
					assert.NoError(t, err)

					_, err = counterStorage.Update(ctx, storage.OperationCounter, big.NewInt(test.operationCount))
					assert.NoError(t, err)

					_, err = counterStorage.Update(ctx, storage.ActiveReconciliationCounter, big.NewInt(test.activeReconciliations))
					assert.NoError(t, err)

					_, err = counterStorage.Update(ctx, storage.InactiveReconciliationCounter, big.NewInt(test.inactiveReconciliations))
					assert.NoError(t, err)
				}

				var balanceStorage *storage.BalanceStorage
				if test.provideBalanceStorage {
					balanceStorage = storage.NewBalanceStorage(localStore)

					dbTransaction := localStore.NewDatabaseTransaction(ctx, true)
					j := 0
					currency := &types.Currency{Symbol: "BLAH"}
					block := &types.BlockIdentifier{Hash: "0", Index: 0}
					for i := 0; i < test.totalAccounts; i++ {
						acct := &types.AccountIdentifier{
							Address: fmt.Sprintf("account %d", i),
						}
						assert.NoError(t, balanceStorage.SetBalance(
							ctx,
							dbTransaction,
							acct,
							&types.Amount{Value: "1", Currency: currency},
							block,
						))

						if j >= test.reconciledAccounts {
							continue
						}

						assert.NoError(t, balanceStorage.Reconciled(
							ctx,
							acct,
							currency,
							block,
						))

						j++
					}

					assert.NoError(t, dbTransaction.Commit(ctx))
				}

				t.Run(testName, func(t *testing.T) {
					results := ComputeCheckDataResults(test.cfg, testErr, counterStorage, balanceStorage)
					assert.Equal(t, test.result, results)
					results.Print() // make sure doesn't panic
					results.Output(logPath)

					var output CheckDataResults
					assert.NoError(t, utils.LoadAndParse(logPath, &output))
					assert.Equal(t, test.result, &output)
				})

				assert.NoError(t, localStore.Close(ctx))
				utils.RemoveTempDir(dir)
			}
		})
	}
}
