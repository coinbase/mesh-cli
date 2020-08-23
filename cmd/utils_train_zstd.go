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

package cmd

import (
	"context"
	"log"
	"path"

	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/spf13/cobra"
)

const (
	trainArgs = 3
)

var (
	utilsTrainZstdCmd = &cobra.Command{
		Use:   "utils:train-zstd",
		Short: "Generate a zstd dictionary for enhanced compression performance",
		Long: `Zstandard (https://github.com/facebook/zstd) is used by
rosetta-sdk-go/storage to compress data stored to disk. It is possible
to improve compression performance by training a dictionary on a particular
storage namespace. This command runs this training and outputs a dictionary
that can be used with rosetta-sdk-go/storage.

The arguments for this command are: <namespace> <database path> <dictionary path>

You can learn more about dictionary compression on the Zstandard
website: https://github.com/facebook/zstd#the-case-for-small-data-compression`,
		Run:  runTrainZstdCmd,
		Args: cobra.ExactArgs(trainArgs),
	}
)

func runTrainZstdCmd(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	namespace := args[0]
	databasePath := path.Clean(args[1])
	dictionaryPath := path.Clean(args[2])

	log.Printf("Running zstd training (this could take a while)...")

	_, _, err := storage.BadgerTrain(ctx, namespace, databasePath, dictionaryPath)
	if err != nil {
		log.Fatal(err)
	}
}
