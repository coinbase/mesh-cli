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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainsString(t *testing.T) {
	var tests = map[string]struct {
		arr []string
		s   string

		contains bool
	}{
		"empty arr": {
			s: "hello",
		},
		"single arr": {
			arr:      []string{"hello"},
			s:        "hello",
			contains: true,
		},
		"single arr no elem": {
			arr: []string{"hello"},
			s:   "test",
		},
		"multiple arr with elem": {
			arr:      []string{"hello", "test"},
			s:        "test",
			contains: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.contains, ContainsString(test.arr, test.s))
		})
	}
}
