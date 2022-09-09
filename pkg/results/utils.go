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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// JSONFetch makes a GET request to the URL and unmarshal
// the response into output.
func JSONFetch(url string, output interface{}) error {
	resp, err := http.Get(url) // #nosec
	if err != nil {
		return fmt.Errorf("unable to fetch url %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received %d status with body %s", resp.StatusCode, string(body))
	}

	if err := json.Unmarshal(body, output); err != nil {
		return fmt.Errorf("unable to unmarshal: %w", err)
	}

	return nil
}
