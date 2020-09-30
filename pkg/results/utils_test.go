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
		expectedError  error
	}{
		"simple 200": {
			status: http.StatusOK,
			body:   `{"test":"123"}`,
			expectedResult: map[string]interface{}{
				"test": "123",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "GET", r.Method)

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, test.body)
			}))
			defer ts.Close()

			var obj map[string]interface{}
			err := JSONFetch(ts.URL, &obj)
			if test.expectedError != nil {
				assert.EqualError(t, test.expectedError, err.Error())
				assert.Len(t, obj, 0)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedResult, obj)
			}
		})
	}
}
