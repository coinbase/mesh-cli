.PHONY: deps lint format check-format test test-cover add-license \
	check-license shorten-lines salus validate watch-blocks \
	watch-transactions watch-balances watch-reconciliations \
	view-block-benchmarks view-account-benchmarks
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
GO_INSTALL=GO111MODULE=off go get
TEST_SCRIPT=go test -v ./internal/...

deps:
	go get ./...
	go get github.com/stretchr/testify
	${GO_INSTALL} github.com/google/addlicense
	${GO_INSTALL} github.com/segmentio/golines
	${GO_INSTALL} github.com/mattn/goveralls

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
	goveralls -coverprofile=c.out -repotoken ${COVERALLS_TOKEN}

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

shorten-lines:
	golines -w --shorten-comments internal cmd

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: add-license shorten-lines format test lint salus

build:
	go build ./...

install:
	go install ./...
