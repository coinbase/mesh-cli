package tester

import (
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/processor"
	"github.com/coinbase/rosetta-cli/pkg/storage"
	"github.com/coinbase/rosetta-cli/pkg/utils"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/stretchr/testify/assert"
)

var (
	t = true
	f = false
)

func TestComputeCheckDataResults(t *testing.T) {
	var tests = map[string]struct {
		cfg *configuration.Configuration

		// We use a slice of errors here because
		// there typically a collection of errors
		// that should return the same result.
		err []error

		counterStorage *storage.CounterStorage
		balanceStorage *storage.BalanceStorage

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

				t.Run(testName, func(t *testing.T) {
					assert.Equal(t, test.result, ComputeCheckDataResults(test.cfg, testErr, test.counterStorage, test.balanceStorage))
				})
			}
		})
	}
}
