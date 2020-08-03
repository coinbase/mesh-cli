<p align="center">
  <a href="https://www.rosetta-api.org">
    <img width="90%" alt="Rosetta" src="https://www.rosetta-api.org/img/rosetta_header.png">
  </a>
</p>
<h3 align="center">
   Rosetta CLI
</h3>
<p align="center">
CLI to validate the correctness of Rosetta API implementations
</p>
<p align="center">
  <a href="https://circleci.com/gh/coinbase/rosetta-cli/tree/master"><img src="https://circleci.com/gh/coinbase/rosetta-cli/tree/master.svg?style=shield" /></a>
  <a href="https://coveralls.io/github/coinbase/rosetta-cli"><img src="https://coveralls.io/repos/github/coinbase/rosetta-cli/badge.svg" /></a>
  <a href="https://goreportcard.com/report/github.com/coinbase/rosetta-cli"><img src="https://goreportcard.com/badge/github.com/coinbase/rosetta-cli" /></a>
  <a href="https://github.com/coinbase/rosetta-cli/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/coinbase/rosetta-cli.svg" /></a>
</p>

## Overview
The `rosetta-cli` is used by developers to test the correctness of their Rosetta
API implementations. The CLI also provides the ability to look up block contents
and account balances.

## Documentation
Before diving into the CLI, we recommend taking a look at the Rosetta API Docs:

* [Overview](https://www.rosetta-api.org/docs/welcome.html)
* [Data API](https://www.rosetta-api.org/docs/data_api_introduction.html)
* [Construction API](https://www.rosetta-api.org/docs/construction_api_introduction.html)

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
  check:construction           Check the correctness of a Rosetta Construction API Implementation
  check:data                   Check the correctness of a Rosetta Data API Implementation
  configuration:create         Create a default configuration file at the provided path
  configuration:validate       Validate the correctness of a configuration file at the provided path
  help                         Help about any command
  utils:asserter-configuration Generate a static configuration file for the Asserter
  version                      Print rosetta-cli version
  view:account                 View an account balance
  view:block                   View a block
  view:networks                View all network statuses

Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
  -h, --help                        help for rosetta-cli

Use "rosetta-cli [command] --help" for more information about a command.
```

### Configuration
All `rosetta-cli` parameters are populated from a configuration file (`--configuration-file`)
provided at runtime. If a configuration file is not provided, the default
configuration is used. This default configuration can be viewed
[here](examples/configuration/default.json).

In the `examples/configuration` directory, you can find examples configuration
files for running tests against a Bitcoin Rosetta implementation
([config](examples/configuration/bitcoin.json)) and an Ethereum Rosetta
implementation ([config](examples/configuration/ethereum.json)).

#### Disable Complex Checks
If you are just getting started with your implementation, you may want
to disable balance tracking (did any address balance go below zero?) and
reconciliation (does the balance I calculated match the balance returned
by the `/account/balance` endpoint?). Take a look at the
[simple configuration](examples/configuration/simple.json) for an example of
how to do this.

#### Future Work
In the near future, we will add support for providing complex exit conditions
(i.e. did we reach tip? did we reconcile every account?) for both
`check:construction` and `check:data` so that the `rosetta-cli`
can be integrated into a CI flow. Currently, the only way to exit with a
successful status in the `rosetta-cli` is to provide an `--end` flag
when running `check:data` (returns 0 if no errors up to a block index
are observed).

### Commands
#### version
```
Print rosetta-cli version

Usage:
  rosetta-cli version [flags]

Flags:
  -h, --help   help for version

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### check:data
```
Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

When re-running this command, it will start where it left off if you specify
some data directory. Otherwise, it will create a new temporary directory and start
again from the genesis block. If you want to discard some number of blocks
populate the --start flag with some block index. Starting from a given index
can be useful to debug a small range of blocks for issues but it is highly
recommended you sync from start to finish to ensure all correctness checks
are performed.

By default, account balances are looked up at specific heights (instead of
only at the current block). If your node does not support this functionality
set historical balance disabled to true. This will make reconciliation much
less efficient but it will still work.

If check fails due to an INACTIVE reconciliation error (balance changed without
any corresponding operation), the cli will automatically try to find the block
missing an operation. If historical balance disabled is true, this automatic
debugging tool does not work.

To debug an INACTIVE account reconciliation error without historical balance lookup,
set the interesting accunts to the path of a JSON file containing
accounts that will be actively checked for balance changes at each block. This
will return an error at the block where a balance change occurred with no
corresponding operations.

If your blockchain has a genesis allocation of funds and you set
historical balance disabled to true, you must provide an
absolute path to a JSON file containing initial balances with the
bootstrap balance config. You can look at the examples folder for an example
of what one of these files looks like.

Usage:
  rosetta-cli check:data [flags]

Flags:
      --end int     block index to stop syncing (default -1)
  -h, --help        help for check:data
      --start int   block index to start syncing (default -1)

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

##### Status Codes
If there are no issues found while running `check`, it will exit with a `0` status code.
If there are any issues, it will exit with a `1` status code. It can be useful
to run this command as an integration test for any changes to your implementation.

#### check:construction
```
The check:construction command runs an automated test of a
Construction API implementation by creating and broadcasting transactions
on a blockchain. In short, this tool generates new addresses, requests
funds, constructs transactions, signs transactions, broadcasts transactions,
and confirms transactions land on-chain. At each phase, a series of tests
are run to ensure that intermediate representations are correct (i.e. does
an unsigned transaction return a superset of operations provided during
construction?).

Check out the https://github.com/coinbase/rosetta-cli/tree/master/examples
directory for examples of how to configure this test for Bitcoin and
Ethereum.

Right now, this tool only supports transfer testing (for both account-based
and UTXO-based blockchains). However, we plan to add support for testing
arbitrary scenarios (i.e. staking, governance).

Usage:
  rosetta-cli check:construction [flags]

Flags:
  -h, --help   help for check:construction

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### configuration:create
```
Create a default configuration file at the provided path

Usage:
  rosetta-cli configuration:create [flags]

Flags:
  -h, --help   help for configuration:create

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### configuration:validate
```
Validate the correctness of a configuration file at the provided path

Usage:
  rosetta-cli configuration:validate [flags]

Flags:
  -h, --help   help for configuration:validate

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### view:networks
```
While debugging a Data API implementation, it can be very
useful to view network(s) status. This command fetches the network
status from all available networks and prints it to the terminal.

If this command errors, it is likely because the /network/* endpoints are
not formatted correctly.

Usage:
  rosetta-cli view:networks [flags]

Flags:
  -h, --help   help for view:networks

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### view:account
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
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### view:block
```
While debugging a Data API implementation, it can be very
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
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

#### utils:asserter-configuration
```
In production deployments, it is useful to initialize the response
Asserter (https://github.com/coinbase/rosetta-sdk-go/tree/master/asserter) using
a static configuration instead of intializing a configuration dynamically
from the node. This allows a client to error on new types/statuses that may
have been added in an update instead of silently erroring.

To use this command, simply provide an absolute path as the argument for where
the configuration file should be saved (in JSON).

Usage:
  rosetta-cli utils:asserter-configuration [flags]

Flags:
  -h, --help   help for utils:asserter-configuration

Global Flags:
      --configuration-file string   Configuration file that provides connection and test settings.
                                    If you would like to generate a starter configuration file (populated
                                    with the defaults), run rosetta-cli configuration:create.

                                    Any fields not populated in the configuration file will be populated with
                                    default values.
```

## Development
* `make deps` to install dependencies
* `make test` to run tests
* `make lint` to lint the source code (included generated code)
* `make release` to run one last check before opening a PR

### Helper/Handler
Many of the packages use a `Helper/Handler` interface pattern to acquire
required information or to send events to some client implementation. An example
of this is in the `reconciler` package where a `Helper` is used to get
the account balance and the `Handler` is called to incidate whether the
reconciliation of an account was successful.

### Repo Structure
```
cmd
examples // examples of different config files
pkg
  logger // logic to write syncing information to stdout/files
  processor // Helper/Handler implementations for reconciler, storage, and syncer
  storage // persists block to temporary storage and allows for querying balances
  utils // useful functions
```

## Correctness Checks
This tool performs a variety of correctness checks using the Rosetta Server. If
any correctness check fails, the CLI will exit and print out a detailed
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
The CLI checks that the balance of an account computed by
its operations is equal to the balance of the account according
to the node. If this balance is not identical, the CLI will
exit.

#### Inactive Addresses
The CLI randomly checks the balances of accounts that aren't
involved in any transactions. The balances of accounts could change
on the blockchain node without being included in an operation
returned by the Rosetta Data API. Recall that all balance-changing
operations should be returned by the Rosetta Data API.

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2020 Coinbase
