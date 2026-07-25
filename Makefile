.PHONY: build test run lint cover docker-build docker-run clean

build:
	go build -o bin/exchange-connectors ./cmd/exchange-connectors

test:
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=./internal/...

run:
	go run ./cmd/exchange-connectors

lint:
	golangci-lint run

cover: test
	go tool cover -func=coverage.out | tail -1

docker-build:
	docker build -t ai-crypto-onramp/exchange-connector .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/exchange-connector

clean:
	rm -rf bin/ coverage.out
