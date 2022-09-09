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

package results

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONFetch(t *testing.T) {
	var tests = map[string]struct {
		status int
		body   string

		expectedResult map[string]interface{}
		expectedError  string
	}{
		"simple 200": {
			status: http.StatusOK,
			body:   `{"test":"123"}`,
			expectedResult: map[string]interface{}{
				"test": "123",
			},
		},
		"not 200": {
			status:        http.StatusUnsupportedMediaType,
			body:          `hello`,
			expectedError: "received 415 status with body hello\n",
		},
		"not JSON": {
			status:        http.StatusOK,
			body:          `hello`,
			expectedError: "unable to unmarshal: invalid character 'h' looking for beginning of value",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "GET", r.Method)

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(test.status)
				fmt.Fprintln(w, test.body)
			}))
			defer ts.Close()

			var obj map[string]interface{}
			err := JSONFetch(ts.URL, &obj)
			if len(test.expectedError) > 0 {
				assert.EqualError(t, err, test.expectedError)
				assert.Len(t, obj, 0)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedResult, obj)
			}
		})
	}
}
