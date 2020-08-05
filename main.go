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

package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/coinbase/rosetta-cli/cmd"
	"github.com/coinbase/rosetta-cli/internal/utils"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go func() {
		var allocated uint64
		for {
			newInterval := utils.PrintMemUsage()
			if newInterval > allocated {
				allocated = newInterval
				log.Printf("New Max Alloc: %d MB", allocated)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
