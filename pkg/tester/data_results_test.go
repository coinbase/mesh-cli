package tester

import (
	"testing"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/storage"

	"github.com/stretchr/testify/assert"
)

func TestComputeCheckDataResults(t *testing.T) {
	var tests = map[string]struct {
		cfg *configuration.Configuration
		err error

		counterStorage *storage.CounterStorage
		balanceStorage *storage.BalanceStorage

		result *CheckDataResults
	}{
		"default configuration, no storage, no error": {
			cfg: configuration.DefaultConfiguration(),
			result: &CheckDataResults{
				Tests: &CheckDataTests{
					Endpoints:         true,
					ResponseAssertion: true,
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.result, ComputeCheckDataResults(test.cfg, test.err, test.counterStorage, test.balanceStorage))
		})
	}
}
