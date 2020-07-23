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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
)

// CreateTempDir creates a directory in
// /tmp for usage within testing.
func CreateTempDir() (string, error) {
	storageDir, err := ioutil.TempDir("", "rosetta-cli")
	if err != nil {
		return "", err
	}

	color.Cyan("Using temporary directory %s", storageDir)
	return storageDir, nil
}

// RemoveTempDir deletes a directory at
// a provided path for usage within testing.
func RemoveTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// Equal returns a boolean indicating if two
// interfaces are equal.
func Equal(a interface{}, b interface{}) bool {
	return types.Hash(a) == types.Hash(b)
}

// LoadAndParse reads the file at the provided path
// and attempts to marshal it into output.
func LoadAndParse(filePath string, output interface{}) error {
	bytes, err := ioutil.ReadFile(path.Clean(filePath))
	if err != nil {
		return fmt.Errorf("%w: unable to load file %s", err, filePath)
	}

	if err := json.Unmarshal(bytes, &output); err != nil {
		return fmt.Errorf("%w: unable to unmarshal", err)
	}

	return nil
}
