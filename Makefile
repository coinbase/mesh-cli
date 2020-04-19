.PHONY: deps lint format check-format test test-cover add-license \
	check-license shorten-lines salus validate watch-blocks \
	watch-transactions watch-balances watch-reconciliations \
	view-block-benchmarks view-account-benchmarks
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
TEST_SCRIPT=go test -v ./internal/...

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get github.com/google/addlicense
	go get github.com/segmentio/golines
	go get github.com/mattn/goveralls

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

watch-blocks:
	tail -f ${PWD}/validator-data/blocks.txt

watch-transactions:
	tail -f ${PWD}/validator-data/transactions.txt

watch-balances:
	tail -f ${PWD}/validator-data/balances.txt

watch-reconciliations:
	tail -f ${PWD}/validator-data/reconciliations.txt
