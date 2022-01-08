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
The rosetta-cli tool is used by developers to test the correctness of their Rosetta API implementations. The CLI also provides the ability to look up block contents and account balances.

## Documentation

For more information on the rosetta-cli tool, read our [The rosetta-cli tool](https://www.rosetta-api.org/docs/rosetta_cli.html) documentation.
For more information on how to test your implementation file with rosetta-cli, read our [How to Test Your Rosetta Implementation](https://www.rosetta-api.org/docs/rosetta_test.html) documentation.

Before diving into the CLI, we recommend taking a look at the [Rosetta API](https://www.rosetta-api.org/docs/welcome.html) documentation.

## Additional Documentation

* [The rosetta-cli tool](https://www.rosetta-api.org/docs/rosetta_cli.html)
    * [Usage](https://www.rosetta-api.org/docs/rosetta_cli.html#usage)
    * [Commands](https://www.rosetta-api.org/docs/rosetta_cli.html#commands)
* [Reference Implementations](https://www.rosetta-api.org/docs/reference_implementations.html)
* [How to Write a Configuration File for rosetta-cli Testing](https://www.rosetta-api.org/docs/rosetta_configuration_file.html)
* [How to Test your Rosetta Implementation](https://www.rosetta-api.org/docs/rosetta_test.html)
* [How to Write a Rosetta DSL File](https://www.rosetta-api.org/docs/rosetta_dsl_file.html)
* [How to Disable Complex Checks](https://www.rosetta-api.org/docs/disable_complex_checks.html)

## Install
To download a binary for the latest release, run:
```
curl -sSfL https://raw.githubusercontent.com/coinbase/rosetta-cli/master/scripts/install.sh | sh -s
```

The binary will be installed inside the `./bin` directory (relative to where the install command was run).

_Downloading binaries from the Github UI will cause permission errors on Mac._

### Installing in Custom Location
To download the binary into a specific directory, run:
```
curl -sSfL https://raw.githubusercontent.com/coinbase/rosetta-cli/master/scripts/install.sh | sh -s -- -b <relative directory>
```

## Development
* `make deps` to install dependencies
* `make test` to run tests
* `make lint` to lint the source code (included generated code)
* `make release` to run one last check before opening a PR
* `make compile version=RELEASE_TAG` to generate binaries

If you are developing on both rosetta-cli and rosetta-sdk-go, use [go.mod replace](https://golang.org/ref/mod#go-mod-file-replace) to reference local changes: 
```
replace "github.com/coinbase/rosetta-sdk-go" v0.6.8 => "../rosetta-sdk-go"
```

### Helper/Handler
Many of the packages use a `Helper/Handler` interface pattern to acquire required information or to send events to some client implementation. An example of this is in the `reconciler` package where a `Helper` is used to get the account balance and the `Handler` is called to indicate whether the reconciliation of an account was successful.

### Repo Structure
```
cmd
examples // examples of different config files
pkg
  logger // logic to write syncing information to stdout/files
  processor // Helper/Handler implementations for reconciler, storage, and syncer
  tester // test orchestrators
```

## Troubleshoot
* While running `check:data` or `check:construction` option if you get the following error:

    ```dial tcp 127.0.0.1:8080: socket: too many open files: unable to sync to 1902533: unable to sync to 1902533```
    
    Please run `ulimit -n 10000` to increase the max concurrent opened file limit

    _Note: MacOS users, if you face  `ulimit: setrlimit failed: invalid argument` error while setting `ulimit`, please run `sudo launchctl limit maxfiles 10000 200000` before setting the `ulimit`_

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2021 Coinbase