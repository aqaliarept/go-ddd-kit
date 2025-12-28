.PHONY: test lint build test-mongo

build:
	go build work

test:
	go test work -race

lint:
	tools/lint.sh

# Temporarily: run only mongo tests (excluding build and lint)
ci: lint build test 