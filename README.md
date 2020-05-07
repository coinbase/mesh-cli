# rosetta-cli

[![Coinbase](https://circleci.com/gh/coinbase/rosetta-cli/tree/master.svg?style=shield)](https://circleci.com/gh/coinbase/rosetta-cli/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/coinbase/rosetta-cli/badge.svg)](https://coveralls.io/github/coinbase/rosetta-cli)
[![Go Report Card](https://goreportcard.com/badge/github.com/coinbase/rosetta-cli)](https://goreportcard.com/report/github.com/coinbase/rosetta-cli)
[![License](https://img.shields.io/github/license/coinbase/rosetta-cli.svg)](https://github.com/coinbase/rosetta-cli/blob/master/LICENSE.txt)

## What is Rosetta?
Rosetta is a new project from Coinbase to standardize the process
of deploying and interacting with blockchains. With an explicit
specification to adhere to, all parties involved in blockchain
development can spend less time figuring out how to integrate
with each other and more time working on the novel advances that
will push the blockchain ecosystem forward. In practice, this means
that any blockchain project that implements the requirements outlined
in this specification will enable exchanges, block explorers,
and wallets to integrate with much less communication overhead
and network-specific work.

## Install
```
go get github.com/coinbase/rosetta-cli
```

## Usage
```
CLI for the Rosetta API

Usage:
  rosetta-cli [command]

Available Commands:
  check                Check the correctness of a Rosetta Node API Server
  create:configuration Generate a static configuration file for the Asserter
  help                 Help about any command
  view:account         View an account balance
  view:block           View a block

Flags:
  -h, --help                help for rosetta-cli
      --server-url string   base URL for a Rosetta server (default "http://localhost:8080")

Use "rosetta-cli [command] --help" for more information about a command.
```

### check
```
Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off if you specify
some --data-dir. Otherwise, it will create a new temporary directory and start
again from the genesis block. If you want to discard some number of blocks
populate the --start flag with some block index. Starting from a given index
can be useful to debug a small range of blocks for issues but it is highly
recommended you sync from start to finish to ensure all correctness checks
are performed.

By default, account balances are looked up at specific heights (instead of
only at the current block). If your node does not support this functionality
set --lookup-balance-by-block to false. This will make reconciliation much
less efficient but it will still work.

To debug an INACTIVE account reconciliation error, set the
--interesting-accounts flag to the absolute path of a JSON file containing
accounts that will be actively checked for balance changes at each block. This
will return an error at the block where a balance change occurred with no
corresponding operations.

If your blockchain has a genesis allocation of funds and you set
--lookup-balance-by-block to false, you must provide an
absolute path to a JSON file containing initial balances with the
--bootstrap-balances flag. You can look at the examples folder for an example
of what one of these files looks like.

Usage:
  rosetta-cli check [flags]

Flags:
      --account-concurrency uint       concurrency to use while fetching accounts during reconciliation (default 8)
      --block-concurrency uint         concurrency to use while fetching blocks (default 8)
      --bootstrap-balances string      Absolute path to a file used to bootstrap balances before starting syncing.
                                       Populating this value after beginning syncing will return an error.
      --data-dir string                folder used to store logs and any data used to perform validation
      --end int                        block index to stop syncing (default -1)
      --exempt-accounts string         Absolute path to a file listing all accounts to exempt from balance
                                       tracking and reconciliation. Look at the examples directory for an example of
                                       how to structure this file.
      --halt-on-reconciliation-error   Determines if block processing should halt on a reconciliation
                                       error. It can be beneficial to collect all reconciliation errors or silence
                                       reconciliation errors during development. (default true)
  -h, --help                           help for check
      --interesting-accounts string    Absolute path to a file listing all accounts to check on each block. Look
                                       at the examples directory for an example of how to structure this file.
      --log-balance-changes            log balance changes
      --log-blocks                     log processed blocks
      --log-reconciliations            log balance reconciliations
      --log-transactions               log processed transactions
      --lookup-balance-by-block        When set to true, balances are looked up at the block where a balance
                                       change occurred instead of at the current block. Blockchains that do not support
                                       historical balance lookup should set this to false. (default true)
      --start int                      block index to start syncing (default -1)
      --transaction-concurrency uint   concurrency to use while fetching transactions (if required) (default 16)

Global Flags:
      --server-url string   base URL for a Rosetta server (default "http://localhost:8080")
```

### create:configuration
```
In production deployments, it is useful to initialize the response
Asserter (https://github.com/coinbase/rosetta-sdk-go/tree/master/asserter) using
a static configuration instead of intializing a configuration dynamically
from the node. This allows a client to error on new types/statuses that may
have been added in an update instead of silently erroring.

To use this command, simply provide an absolute path as the argument for where
the configuration file should be saved (in JSON). Populate the optional
--server-url flag with the url of the server to generate the configuration
from.

Usage:
  rosetta-cli create:configuration [flags]

Flags:
  -h, --help   help for create:configuration

Global Flags:
      --server-url string   base URL for a Rosetta server (default "http://localhost:8080")
```

### view:account
```
While debugging, it is often useful to inspect the state
of an account at a certain block. This command allows you to look up
any account by providing a JSON representation of a types.AccountIdentifier
(and optionally a height to perform the query).

For example, you could run view:account '{"address":"interesting address"}' 1000
to lookup the balance of an interesting address at block 1000. Allowing the
address to specified as JSON allows for querying by SubAccountIdentifier.

Usage:
  rosetta-cli view:account [flags]

Flags:
  -h, --help   help for view:account

Global Flags:
      --server-url string   base URL for a Rosetta server (default "http://localhost:8080")
```

### view:block
```
While debugging a Node API implementation, it can be very
useful to inspect block contents. This command allows you to fetch any
block by index to inspect its contents. It uses the
fetcher (https://github.com/coinbase/rosetta-sdk-go/tree/master/fetcher) package
to automatically get all transactions in the block and assert the format
of the block is correct before printing.

If this command errors, it is likely because the block you are trying to
fetch is formatted incorrectly.

Usage:
  rosetta-cli view:block [flags]

Flags:
  -h, --help   help for view:block

Global Flags:
      --server-url string   base URL for a Rosetta server (default "http://localhost:8080")
```

## Development
* `make deps` to install dependencies
* `make test` to run tests
* `make lint` to lint the source code (included generated code)
* `make release` to run one last check before opening a PR

### Helper/Handler
Many of the internal packages use a `Helper/Handler` interface pattern to acquire
required information or to send events to some client implementation. An example
of this is in the `internal/reconciler` package where a `Helper` is used to get
the account balance and the `Handler` is called to incidate whether the
reconciliation of an account was successful.

### Repo Structure
```
cmd
examples // examples of different config files
internal
  logger // logic to write syncing information to stdout/files
  processor // Helper/Handler implementations for reconciler, storage, and syncer
  reconciler // checks for equality between computed balance and node balance
  storage // persists block to temporary storage and allows for querying balances
  syncer // coordinates block syncing (inlcuding re-orgs)
  utils // useful functions
```

## Correctness Checks
This tool performs a variety of correctness checks using the Rosetta Server. If
any correctness check fails, the validator will exit and print out a detailed
message explaining the error.

### Response Correctness
The validator uses the autogenerated [Go Client package](https://github.com/coinbase/rosetta-sdk-go)
to communicate with the Rosetta Server and assert that responses adhere
to the Rosetta interface specification.

### Duplicate Hashes
The validator checks that a block hash or transaction hash is
never duplicated.

### Non-negative Balances
The validator checks that an account balance does not go
negative from any operations.

### Balance Reconciliation
#### Active Addresses
The validator checks that the balance of an account computed by
its operations is equal to the balance of the account according
to the node. If this balance is not identical, the validator will
exit.

#### Inactive Addresses
The validator randomly checks the balances of accounts that aren't
involved in any transactions. The balances of accounts could change
on the blockchain node without being included in an operation
returned by the Rosetta Server. Recall that all balance-changing
operations must be returned by the Rosetta Server.

## Future Work
* Move syncer, reconciler, and storage packages to rosetta-sdk-go for better re-use.
* Automatically test the correctness of a Rosetta Client SDK by constructing,
signing, and submitting a transaction. This can be further extended by ensuring
broadcast transactions eventually land in a block.
* Change logging to utilize a more advanced output mechanism than CSV.

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2020 Coinbase
