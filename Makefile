.PHONY: deps lint test add-license check-license circleci-local validator \
	watch-blocks view-block-benchmarks view-account-benchmarks salus
LICENCE_SCRIPT=addlicense -c "Coinbase, Inc." -l "apache" -v
SERVER_URL?=http://localhost:10000
LOG_TRANSACTIONS?=false
LOG_BENCHMARKS?=true
BOOTSTRAP_BALANCES?=false

deps:
	go get ./...
	go get github.com/stretchr/testify
	go get golang.org/x/lint/golint
	go get github.com/google/addlicense

lint:
	golint ./internal/...

test:
	go test -v ./internal/...

add-license:
	${LICENCE_SCRIPT} .

check-license:
	${LICENCE_SCRIPT} -check .

circleci-local:
	circleci local execute

salus:
	docker run --rm -t -v ${PWD}:/home/repo coinbase/salus

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
		-e BOOTSTRAP_BALANCES="${BOOTSTRAP_BALANCES}" \
		--network host \
		rosetta-validator \
		rosetta-validator;

watch-blocks:
	tail -f ${PWD}/validator-data/blocks.txt

view-block-benchmarks:
	open ${PWD}/validator-data/block_benchmarks.csv

view-account-benchmarks:
	open ${PWD}/validator-data/account_benchmarks.csv
