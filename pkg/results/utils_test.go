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
			expectedError: "invalid character 'h' looking for beginning of value: unable to unmarshal JSON",
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
