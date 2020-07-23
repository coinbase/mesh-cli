package configuration

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/coinbase/rosetta-cli/internal/utils"

	// "github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfiguration(t *testing.T) {
	var tests = map[string]struct {
		provided *Configuration
		expected *Configuration
	}{
		"nothing provided": {
			provided: &Configuration{},
			expected: DefaultConfiguration(),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Write configuration file to tempdir
			tmpfile, err := ioutil.TempFile("", "test.json")
			assert.NoError(t, err)
			defer os.Remove(tmpfile.Name())

			err = utils.SerializeAndWrite(tmpfile.Name(), test.provided)
			assert.NoError(t, err)

			// Check if expected fields populated
			config, err := LoadConfiguration(tmpfile.Name())
			assert.NoError(t, err)
			assert.Equal(t, test.expected, config)
			assert.NoError(t, tmpfile.Close())
		})
	}
}
