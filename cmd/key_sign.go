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
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	keySignCmd = &cobra.Command{
		Use:   "key:sign",
		Short: "Sign an unsigned payload with given private key",
		Long: `Sign an unsigned payload with given private key
				It supports Keypair specified by https://github.com/coinbase/rosetta-specifications
				Please provide valid PrivateKey, CurveType, SignaturePayload`,
		RunE: runKeySignCmd,
	}
)

func runKeySignCmd(_ *cobra.Command, _ []string) error {
	if Config.Sign == nil {
		return errors.New("sign configuration is missing")
	}

	if len(Config.Sign.PrivateKey) == 0 ||
		Config.Sign.PubKey.CurveType == "" ||
		Config.Sign.SigningPayload == nil ||
		Config.Sign.SigningPayload.SignatureType == "" {
		color.Red("invalid sign input")
	}

	keyPair, err := keys.ImportPrivateKey(Config.Sign.PrivateKey, Config.Sign.PubKey.CurveType)
	if err != nil {
		color.Red("unable to import private keys %#v", err)
		return err
	}

	err = keyPair.IsValid()
	if err != nil {
		color.Red("keypair invalid with err %#v", err)
		return err
	}

	signer, err := keyPair.Signer()
	if err != nil {
		color.Red("signer invalid with err %#v", err)
		return err
	}

	signingPayload := Config.Sign.SigningPayload
	signatureType := Config.Sign.SigningPayload.SignatureType

	sign, err := signer.Sign(signingPayload, signatureType)
	if err != nil {
		color.Red("unable to sign with err %#v", err)
		return err
	}

	hexSig := hex.EncodeToString(sign.Bytes)
	color.Green("Signature: %s", hexSig)
	return nil
}
