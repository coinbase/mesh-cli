.PHONY: deps lint format check-format test test-cover add-license \
	check-license shorten-lines salus validate watch-blocks \
	watch-transactions watch-balances watch-reconciliations \
	view-block-benchmarks view-account-benchmarks mocks

# To run the the following packages as commands,
# it is necessary to use `go run <pkg>`. Running `go get` does
# not install any binaries that could be used to run
# the commands directly.
ADDLICENSE_INSTALL=go install github.com/google/addlicense@latest
ADDLICENSE_CMD=addlicense
ADDLICENSE_IGNORE=-ignore ".github/**/*" -ignore ".idea/**/*"
ADDLICENCE_SCRIPT=${ADDLICENSE_CMD} -c "Coinbase, Inc." -l "apache" -v ${ADDLICENSE_IGNORE}
GOLINES_INSTALL=go install github.com/segmentio/golines@latest
GOLINES_CMD=golines
GOVERALLS_INSTALL=go install github.com/mattn/goveralls@latest
GOVERALLS_CMD=goveralls
COVERAGE_TEST_DIRECTORIES=./configuration/... ./pkg/results/... \
	./pkg/logger/... ./cmd
TEST_SCRIPT=go test -v ./pkg/... ./configuration/... ./cmd
COVERAGE_TEST_SCRIPT=go test -v ${COVERAGE_TEST_DIRECTORIES}

deps:
	go get ./...

lint:
	golangci-lint run --timeout 2m0s -v \
		-E golint,misspell,gocyclo,whitespace,goconst,gocritic,gocognit,bodyclose,unconvert,lll,unparam,gomnd;

format:
	gofmt -s -w -l .;

check-format:
	! gofmt -s -l . | read;

validate-configuration-files:
	go run main.go configuration:validate examples/configuration/simple.json;
	go run main.go configuration:create examples/configuration/default.json;
	go run main.go configuration:validate examples/configuration/default.json;
	git diff --exit-code;

test: | validate-configuration-files
	${TEST_SCRIPT}

test-cover:
	${GOVERALLS_INSTALL}
	if [ "${COVERALLS_TOKEN}" ]; then ${COVERAGE_TEST_SCRIPT} -coverprofile=c.out -covermode=count; ${GOVERALLS_CMD} -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}; fi

add-license:
	${ADDLICENSE_INSTALL}
	${ADDLICENCE_SCRIPT} .

check-license:
	${ADDLICENSE_INSTALL}
	${ADDLICENCE_SCRIPT} -check .

shorten-lines:
	${GOLINES_INSTALL}
	${GOLINES_CMD} -w --shorten-comments pkg cmd configuration

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: add-license shorten-lines format test lint salus

# This command is to generate multi-platform binaries.
compile:
	./scripts/compile.sh $(version)

build:
	go build ./...

install:
	go install ./...
