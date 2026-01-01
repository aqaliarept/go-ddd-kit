.PHONY: test lint build test-mongo test-coverage coverage-html

build:
	go build work

test:
	go test work -race

test-coverage:
	go test work -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html

lint:
	tools/lint.sh

ci: lint build test 