# rosetta-validator

[![Coinbase](https://circleci.com/gh/coinbase/rosetta-validator/tree/master.svg?style=shield)](https://circleci.com/gh/coinbase/rosetta-validator/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/coinbase/rosetta-validator/badge.svg)](https://coveralls.io/github/coinbase/rosetta-validator)
[![Go Report Card](https://goreportcard.com/badge/github.com/coinbase/rosetta-validator)](https://goreportcard.com/report/github.com/coinbase/rosetta-validator)
[![License](https://img.shields.io/github/license/coinbase/rosetta-validator.svg)](https://github.com/coinbase/rosetta-validator/blob/master/LICENSE.txt)

Once you create a Rosetta Server, you'll need to test its
performance and correctness. This validation tool makes that easy!

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

## Run the Validator
1. Start your Rosetta Server (and the blockchain node it connects to if it is
not a single binary.
2. Start the validator using `makevalidate`.
3. Examine processed blocks using `make watch-blocks`.
4. Watch for errors in the processing logs. Any error will cause validation to
stop.

## rosetta-validator
```
A simple CLI to validate a Rosetta server

Usage:
  rosetta-validator [command]

Available Commands:
  check:complete Run a full check of the correctness of a Rosetta server
  check:quick    Run a simple check of the correctness of a Rosetta server
  help           Help about any command
```

## check:complete
```
Check all server responses are properly constructed, that
there are no duplicate blocks and transactions, that blocks can be processed
from genesis to the current block (re-orgs handled automatically), and that
computed balance changes are equal to balance changes reported by the node.

Usage:
  rosetta-validator check:complete [flags]

Flags:
      --bootstrap-balances string   Absolute path to a file used to bootstrap balances before starting syncing.
                                    Populating this value after beginning syncing will return an error.
  -h, --help                        help for check:complete
      --lookup-balance-by-block     When set to true, balances are looked up at the block where a balance
                                    change occurred instead of at the current block. Blockchains that do not support
                                    historical balance lookup should set this to false. (default true)

Global Flags:
      --account-concurrency uint       concurrency to use while fetching accounts during reconciliation (default 8)
      --block-concurrency uint         concurrency to use while fetching blocks (default 8)
      --data-dir string                folder used to store logs and any data used to perform validation (default "./validator-data")
      --end int                        block index to stop syncing (default -1)
      --halt-on-reconciliation-error   Determines if block processing should halt on a reconciliation
                                       error. It can be beneficial to collect all reconciliation errors or silence
                                       reconciliation errors during development. (default true)
      --log-balance-changes            log balance changes (default true)
      --log-blocks                     log processed blocks (default true)
      --log-reconciliations            log balance reconciliations (default true)
      --log-transactions               log processed transactions (default true)
      --server-url string              base URL for a Rosetta server to validate (default "http://localhost:8080")
      --start int                      block index to start syncing (default -1)
      --transaction-concurrency uint   concurrency to use while fetching transactions (if required) (default 16)
```

## check:quick
```
Check all server responses are properly constructed and that
computed balance changes are equal to balance changes reported by the
node. To use check:quick, your server must implement the balance lookup
by block. Unlike check:complete, which requires syncing all blocks up
to the blocks you want to check, check:quick allows you to validate
an arbitrary range of blocks (even if earlier blocks weren't synced).

It is important to note that check:quick does not support re-orgs and it
does not check for duplicate blocks and transactions. For these features,
please use check:complete.

Usage:
  rosetta-validator check:quick [flags]

Flags:
  -h, --help   help for check:quick

Global Flags:
      --account-concurrency uint       concurrency to use while fetching accounts during reconciliation (default 8)
      --block-concurrency uint         concurrency to use while fetching blocks (default 8)
      --data-dir string                folder used to store logs and any data used to perform validation (default "./validator-data")
      --end int                        block index to stop syncing (default -1)
      --halt-on-reconciliation-error   Determines if block processing should halt on a reconciliation
                                       error. It can be beneficial to collect all reconciliation errors or silence
                                       reconciliation errors during development. (default true)
      --log-balance-changes            log balance changes (default true)
      --log-blocks                     log processed blocks (default true)
      --log-reconciliations            log balance reconciliations (default true)
      --log-transactions               log processed transactions (default true)
      --server-url string              base URL for a Rosetta server to validate (default "http://localhost:8080")
      --start int                      block index to start syncing (default -1)
      --transaction-concurrency uint   concurrency to use while fetching transactions (if required) (default 16)
```

### Configuration Options
All configuration options can be set in the call to `make validate`
(ex: `make SERVER_URL=http://localhost:9999 validate`) or altered in
the `Makefile` itself.

_There is no additional setting required to support blockchains with reorgs. This
is handled automatically!_

#### SERVER_URL
_Default: http://localhost:8080_

The URL the validator will use to access the Rosetta Server.

#### RECONCILE_BALANCES
_Default: true_

Computed balances will be reconciled against balances returned by the node.

#### LOG_TRANSACTIONS
_Default: true_

All processed transactions will be logged to `transactions.txt`. You can tail
these logs using `watch-transactions`.

#### LOG_BALANCES
_Default: true_

All processed balance changes will be logged to `balances.txt`. You can tail
these logs using `watch-balances`.

#### LOG_RECONCILIATION
_Default: true_

All reconciliation checks will be logged to `reconciliations.txt`. You can tail
these logs using `watch-reconciliations`.

#### BOOTSTRAP_BALANCES
_Default: false_

Blockchains that set balances in genesis must create a `bootstrap_balances.csv`
file in the `/validator-data` directory and pass `BOOTSTRAP_BALANCES=true` as an
argument to make. If balances are not bootsrapped and balances are set in genesis,
reconciliation will fail.

There is an example file in `examples/bootstrap_balances.csv`.

#### NEW_HEAD_INDEX
_Default: -1_

When debugging a server, it can be useful to revert some number of erroneous blocks
instead of starting validation over. To do so, set the `NEW_HEAD_INDEX` to the value
of the last correct block.

#### LOOKUP_BALANCE_BY_BLOCK
_Default: true_

It is much more efficient to reconcile balances when it is possible to lookup
balances at a particular block. However, if your server does not support
historical balance lookup, you should disable this option. Reconciliation will
still be performed but it will rely on fetching the current balances of
interesting accounts.

## Development
* `make deps` to install dependencies
* `make test` to run tests
* `make lint` to lint the source code (included generated code)
* `make release` to run one last check before opening a PR

## Correctness Checks
This tool performs a variety of correctness checks using the Rosetta Server. If
any correctness check fails, the validator will exit and print out a detailed
message explaining the error.

### Response Correctness
The validator uses the autogenerated [Go Client package](https://github.com/coinbase/rosetta-sdk-go)
to communicate with the Rosetta Server and assert that responses adhere
to the Rosetta Standard.

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
returned by the Rosetta Server. Recall that **ALL** balance-changing
operations must be returned by the Rosetta Server.

## Future Work
* Automatically test the correctness of a Rosetta Client SDK by constructing,
signing, and submitting a transaction. This can be further extended by ensuring
broadcast transactions eventually land in a block.
* Change logging to utilize a more advanced output mechanism than CSV.

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2020 Coinbase
