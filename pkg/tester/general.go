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

package tester

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/coinbase/rosetta-cli/pkg/logger"
)

const (
	// MemoryLoggingFrequency is the frequency that memory
	// usage stats are logged to the terminal.
	MemoryLoggingFrequency = 10 * time.Second

	// ReadHeaderTimeout is the header timeout for server
	ReadHeaderTimeout = 5 * time.Second
)

// LogMemoryLoop runs a loop that logs memory usage.
func LogMemoryLoop(
	ctx context.Context,
) error {
	ticker := time.NewTicker(MemoryLoggingFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogMemoryStats(ctx)
			return ctx.Err()
		case <-ticker.C:
			logger.LogMemoryStats(ctx)
		}
	}
}

// StartServer stats a server at a port with a particular handler.
// This is often used to support a status endpoint for a particular test.
func StartServer(
	ctx context.Context,
	name string,
	handler http.Handler,
	port uint,
) error {
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
	}

	go func() {
		log.Printf("%s server running on port %d\n", name, port)
		_ = server.ListenAndServe()
	}()

	go func() {
		// If we don't shutdown server, it will
		// never stop because server.ListenAndServe doesn't
		// take any context.
		<-ctx.Done()
		log.Printf("%s server shutting down", name)

		_ = server.Shutdown(ctx)
	}()

	return ctx.Err()
}
