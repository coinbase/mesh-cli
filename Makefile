.PHONY: deps lint format check-format test test-cover add-license \
	check-license shorten-lines salus validate watch-blocks \
	watch-transactions watch-balances watch-reconciliations \
	view-block-benchmarks view-account-benchmarks mocks

# To run the the following packages as commands,
# it is necessary to use `go run <pkg>`. Running `go get` does
# not install any binaries that could be used to run
# the commands directly.
ADDLICENSE_CMD=go run github.com/google/addlicense
ADDLICENCE_SCRIPT=${ADDLICENSE_CMD} -c "Coinbase, Inc." -l "apache" -v
GOLINES_CMD=go run github.com/segmentio/golines
GOVERALLS_CMD=go run github.com/mattn/goveralls
COVERAGE_TEST_DIRECTORIES=./configuration/... ./internal/constructor/... \
													./internal/logger/... ./internal/scenario/... \
													./internal/statefulsyncer/... ./internal/storage/... \
													./internal/tester/... ./internal/utils/...
TEST_SCRIPT=go test -v ./internal/... ./configuration/...
COVERAGE_TEST_SCRIPT=go test -v ${COVERAGE_TEST_DIRECTORIES}

deps:
	go get ./...

lint:
	golangci-lint run -v \
		-E golint,misspell,gocyclo,whitespace,goconst,gocritic,gocognit,bodyclose,unconvert,lll,unparam,gomnd;

format:
	gofmt -s -w -l .;

check-format:
	! gofmt -s -l . | read;

test:
	${TEST_SCRIPT}

test-cover:	
	if [ "${COVERALLS_TOKEN}" ]; then ${COVERAGE_TEST_SCRIPT} -coverprofile=c.out -covermode=count; ${GOVERALLS_CMD} -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}; fi

add-license:
	${ADDLICENCE_SCRIPT} .

check-license:
	${ADDLICENCE_SCRIPT} -check .

shorten-lines:
	${GOLINES_CMD} -w --shorten-comments internal cmd configuration

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: add-license shorten-lines format test lint salus

build:
	go build ./...

install:
	go install ./...

mocks:
	rm -rf mocks;
	mockery --dir internal/constructor --all --case underscore --outpkg constructor --output mocks/constructor;
