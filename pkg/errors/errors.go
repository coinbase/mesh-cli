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

package errors

import (
	"errors"
)

// Configuration Errors

var (
	// Construction Configuration Errors

	ErrParseFileFailed           = errors.New("unable to parse config files")
	ErrBalanceTrackingDisabled   = errors.New("balance tracking disabled")
	ErrReconciliationConfig      = errors.New("invalid reconciliation error")
	ErrCompileDSLFileFailed      = errors.New("unable to compile DSL file")
	ErrParseWorkflowFailed       = errors.New("unable to parse workflow")
	ErrConstructionConfigMissing = errors.New("construction configuration is missing")

	// TODO: Data Configuration Errors

	// Construction check errors

	ErrConstructionCheckHalt = errors.New("construction check halted")

	ErrAsserterConfigError = errors.New("asserter configuration validation failed")

	// Bad Command Errors

	ErrBlockNotFound      = errors.New("block not found")
	ErrNoAvailableNetwork = errors.New("no networks available")
)
