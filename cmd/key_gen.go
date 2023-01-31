// Copyright 2023 Coinbase, Inc.
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

package cmd

import (
	"encoding/hex"
	"errors"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	keyGenCmd = &cobra.Command{
		Use:   "key:gen",
		Short: "Used to generate a public private key pair",
		Long: `Used to generate a public private key pair
				It supports Keypair specified by https://github.com/coinbase/rosetta-specifications
				Please provide valid CurveType`,
		RunE: runKeyGenCmd,
	}
)

func runKeyGenCmd(_ *cobra.Command, _ []string) error {
	if len(curveType) == 0 {
		color.Red("please provide a non-empty curve type")
		return errors.New("invalid curve-type string")
	}

	curve := types.CurveType(curveType)

	color.Yellow("Generating new %s keypair...", curve)
	keyPair, err := keys.GenerateKeypair(curve)
	if err != nil {
		color.Red("failed to generate keypair with error %#v", err)
	}

	color.Green("CurveType: %s", curve)
	color.Green("Public Key (hex): %s", hex.EncodeToString(keyPair.PublicKey.Bytes))
	color.Green("Private Key (hex): %s", hex.EncodeToString(keyPair.PrivateKey))
	return nil
}
