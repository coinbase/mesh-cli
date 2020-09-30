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

// JSONFetch makes a GET request to the URL and marshals
// the response into output.
func JSONFetch(URL string, output interface{}) error {
	resp, err := http.Get(URL)
	if err != nil {
		return fmt.Errorf("%w: unable to fetch GET %s", err, URL)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%w: unable to read body", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received %d status with body %s", resp.StatusCode, body)
	}

	if err := json.Unmarshal(body, output); err != nil {
		return fmt.Errorf("%w: unable to unmarshal JSON", err)
	}

	return nil
}
