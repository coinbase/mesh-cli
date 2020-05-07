.PHONY: deps lint format check-format test test-cover add-license \
	check-license shorten-lines salus validate watch-blocks \
	watch-transactions watch-balances watch-reconciliations \
	view-block-benchmarks view-account-benchmarks
ADDLICENSE_CMD=go run github.com/google/addlicense
ADDLICENCE_SCRIPT=${ADDLICENSE_CMD} -c "Coinbase, Inc." -l "apache" -v
GOLINES_CMD=go run github.com/segmentio/golines
GOVERALLS_CMD=go run github.com/mattn/goveralls
TEST_SCRIPT=go test -v ./internal/...

deps:
	go get ./...

lint:
	golangci-lint run -v \
		-E golint,misspell,gocyclo,whitespace,goconst,gocritic,gocognit,bodyclose,unconvert,lll,unparam,gomnd

format:
	gofmt -s -w -l .

check-format:
	! gofmt -s -l . | read

test:
	${TEST_SCRIPT}

test-cover:	
	${TEST_SCRIPT} -coverprofile=c.out -covermode=count
	${GOVERALLS_CMD} -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}

add-license:
	${ADDLICENCE_SCRIPT} .

check-license:
	${ADDLICENCE_SCRIPT} -check .

shorten-lines:
	${GOLINES_CMD} -w --shorten-comments internal cmd

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: add-license shorten-lines format test lint salus

build:
	go build ./...

install:
	go install ./...
