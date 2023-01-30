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

The `rosetta-cli` tool is used by developers to test the correctness of their Rosetta API implementations. The CLI also provides the ability to look up block contents and account balances.

## Installation

To download a binary for the latest release, run:
```
curl -sSfL https://raw.githubusercontent.com/coinbase/rosetta-cli/master/scripts/install.sh | sh -s
```

The binary will be installed inside the `./bin` directory (relative to the directory where you ran the installation command).

_Downloading binaries from the Github UI will cause permission errors on Mac._

### Installing in a Custom Location
To download the binary into a specific directory, run:
```
curl -sSfL https://raw.githubusercontent.com/coinbase/rosetta-cli/master/scripts/install.sh | sh -s -- -b <relative directory>
```

### Run via Docker
Running the following command will start a Docker container and present CLI for the Rosetta API.
```
docker run -it [image-name] [command]
```

Example: To validate that the Data API implementation is correct, running the following command will start a Docker container with a data directory at `<working directory>`.
```
docker run -v "$(pwd):/data" -it [image-name] check:data --configuration-file /data/config.json
```

## Key Sign Tool
Rosetta CLI comes with a handy key sign tool for local testing. Please refer to this [README](./cmd/README.md) on how to use it.

## Updates and Releases

We recommend that you continually update your installation to the latest release as soon as possible. The latest release notes are available in our [Community](https://community.rosetta-api.org) board under the [Release](https://community.rosetta-api.org/c/releases/13) category.

You can also view releases and change log information in the [Releases](https://github.com/coinbase/rosetta-cli/releases) section of this repository.

## Documentation

You can find the Rosetta API documentation at [rosetta-api.org](https://www.rosetta-api.org/docs/welcome.html)

For more information on the rosetta-cli tool, read our [The rosetta-cli tool](https://www.rosetta-api.org/docs/rosetta_cli.html) documentation.

For more information on how to test your implementation file with the `rosetta-cli` tool, read our [How to Test Your Rosetta Implementation](https://www.rosetta-api.org/docs/rosetta_test.html) documentation.

Our documentation is divided into the following sections:

* [Product Overview](https://www.rosetta-api.org/docs/welcome.html)
* [Getting Started](https://www.rosetta-api.org/docs/getting_started.html)
* [Rosetta API Spec](https://www.rosetta-api.org/docs/Reference.html)
* [Testing](https://www.rosetta-api.org/docs/rosetta_cli.html)
* [Best Practices](https://www.rosetta-api.org/docs/node_deployment.html)
* [Repositories](https://www.rosetta-api.org/docs/rosetta_specifications.html)

## Contributing

You may contribute to the `rosetta-cli` project in various ways:

* [Asking Questions](CONTRIBUTING.md/#asking-questions)
* [Providing Feedback](CONTRIBUTING.md/#providing-feedback)
* [Reporting Issues](CONTRIBUTING.md/#reporting-issues)

Read our [Contributing](CONTRIBUTING.MD) documentation for more information.

## rosetta-cli Tool Development

While working on improvements to this repository, we recommend that you use these commands to check your code:

* `make deps` to install dependencies
* `make test` to run tests
* `make lint` to lint the source code (included generated code)
* `make release` to run one last check before opening a PR
* `make compile version=RELEASE_TAG` to generate binaries

If you are developing on both the `rosetta-cli` and `rosetta-sdk-go` repositories, use [go.mod replace](https://golang.org/ref/mod#go-mod-file-replace) to reference local changes:
```
replace "github.com/coinbase/rosetta-sdk-go" v0.6.8 => "<PATH TO LOCAL rosetta-sdk-go>"
```
### Release
* When we release a new rosetta-cli version, please update the version number to follow [PR](https://github.com/coinbase/rosetta-cli/pull/334) so that `rosetta-cli version` command can print the correct value.
* Create binaries and upload all the binaries in the new release tag (e.g. https://github.com/coinbase/rosetta-cli/releases/tag/v0.7.7)
    * Ensure `$GOPATH/bin` is added to `$PATH`
    * Run `make compile version=<New Version>`
    * All the binaries will be created in the `bin` folder and should have extension as `tar.gz` and new version number

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

### Troubleshooting

While running the `check:data` or `check:construction` option, if you get the following error:

```dial tcp 127.0.0.1:8080: socket: too many open files: unable to sync to 1902533: unable to sync to 1902533```
    
Please run the `ulimit -n 10000` command to increase the max concurrent opened file limit.

_Note: MacOS users, if you face  `ulimit: setrlimit failed: invalid argument` error while setting `ulimit`, please run `sudo launchctl limit maxfiles 10000 200000` before setting the `ulimit`._

## Related Projects

* [`rosetta-sdk-go`](https://github.com/coinbase/rosetta-sdk-go) — The `rosetta-sdk-go` SDK provides a collection of packages used for interaction with the Rosetta API specification. Much of the SDK code is generated from this, the [`rosetta-specifications`](https://github.com/coinbase/rosetta-specifications) repository.
* [`rosetta-specifications`](https://github.com/coinbase/rosetta-specifications) — The `rosetta-specifications` repository generates the SDK code in the [`rosetta-sdk-go`](https://github.com/coinbase/rosetta-sdk-go) repository.

### Reference Implementations

To help you with examples, we developed complete Rosetta API reference implementations for [Bitcoin](https://github.com/coinbase/rosetta-bitcoin) and [Ethereum](https://github.com/coinbase/rosetta-ethereum). Developers of Bitcoin-like or Ethereum-like blockchains may find it easier to fork these reference implementations than to write an implementation from scratch.

You can also find community implementations for a variety of blockchains in the [rosetta-ecosystem](https://github.com/coinbase/rosetta-ecosystem) repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. 


## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).
© 2022 Coinbase