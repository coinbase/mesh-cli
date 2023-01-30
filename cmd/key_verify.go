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
	"errors"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	keyVerifyCmd = &cobra.Command{
		Use:   "key:verify",
		Short: "Verify the signature using the public key",
		Long: `Verify the signature using the public key
				It supports Keypair specified by https://github.com/coinbase/rosetta-specifications`,
		RunE: runKeyVerifyCmd,
	}
)

func runKeyVerifyCmd(_ *cobra.Command, _ []string) error {
	if Config.Sign == nil {
		return errors.New("sign configuration is missing")
	}

	if len(Config.Sign.Signature.Bytes) == 0 ||
		Config.Sign.SigningPayload == nil ||
		Config.Sign.SigningPayload.SignatureType == "" ||
		Config.Sign.PubKey == nil {
		color.Red("invalid verify input")
	}

	keyPair := keys.KeyPair{
		PublicKey: Config.Sign.PubKey,
	}

	signer, err := keyPair.Signer()
	if err != nil {
		color.Red("signer invalid with err %#v", err)
		return err
	}

	signature := Config.Sign.Signature
	signature.SignatureType = Config.Sign.SigningPayload.SignatureType
	signature.SigningPayload = Config.Sign.SigningPayload
	signature.PublicKey = Config.Sign.PubKey

	err = signer.Verify(signature)
	if err != nil {
		color.Red("invalid signature with err %#v", err)
		return err
	}

	color.Green("Signature Verified.")
	return nil
}
