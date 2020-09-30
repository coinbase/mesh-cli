package results

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// JSONFetch makes a GET request to the URL and marshals
// the response into output.
func JSONFetch(url string, output interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("%w: unable to fetch GET %s", err, url)
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
