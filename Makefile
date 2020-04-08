.PHONY: deps lint test add-license check-license circleci-local validator \
	watch-blocks view-block-benchmarks view-account-benchmarks salus
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
TEST_SCRIPT=go test -v ./internal/...

SERVER_URL?=http://localhost:10000
LOG_TRANSACTIONS?=true
LOG_BENCHMARKS?=false
LOG_BALANCES?=true
BOOTSTRAP_BALANCES?=false

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get github.com/google/addlicense
	go get github.com/segmentio/golines
	go get github.com/mattn/goveralls

lint:
	golangci-lint run -v \
		-E golint,misspell,gocyclo,whitespace,goconst,gocognit,bodyclose,unconvert,lll,unparam,gomnd

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
	golines -w --shorten-comments internal 

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

release: add-license shorten-lines format test lint salus

validate:
	docker build -t rosetta-validator .; \
	docker run \
		--rm \
		-v ${PWD}/validator-data:/data \
		-e DATA_DIR="/data" \
		-e SERVER_URL="${SERVER_URL}" \
		-e BLOCK_CONCURRENCY="32" \
		-e TRANSACTION_CONCURRENCY="8" \
		-e ACCOUNT_CONCURRENCY="8" \
		-e LOG_TRANSACTIONS="${LOG_TRANSACTIONS}" \
		-e LOG_BENCHMARKS="${LOG_BENCHMARKS}" \
		-e LOG_BALANCES="${LOG_BALANCES}" \
		-e BOOTSTRAP_BALANCES="${BOOTSTRAP_BALANCES}" \
		--network host \
		rosetta-validator \
		rosetta-validator;

watch-blocks:
	tail -f ${PWD}/validator-data/blocks.txt

watch-balances:
	tail -f ${PWD}/validator-data/balances.txt

watch-transactions:
	tail -f ${PWD}/validator-data/transactions.txt

view-block-benchmarks:
	open ${PWD}/validator-data/block_benchmarks.csv

view-account-benchmarks:
	open ${PWD}/validator-data/account_benchmarks.csv
