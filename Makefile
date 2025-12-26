.PHONY: test lint build

build:
	go build work

test:
	go test work -race

lint:
	tools/lint.sh

ci: build lint test



