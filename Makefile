.PHONY: dev build test cover lint chaos demo docker

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

dev:
	go run ./cmd/stepwise dev

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/stepwise ./cmd/stepwise

test:
	go test -race -cover ./...

cover:
	go test -race -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt -w ." && exit 1)
	go vet ./...

# Kill workers at random while 1,000 agents run; fails if any run is lost or any refund is doubled.
chaos:
	go run ./cmd/chaos -runs 1000 -workers 8 -kill-every 120ms -latency 40ms -lease 300ms -out docs/chaos-report.md

demo:
	./examples/demo.sh

docker:
	docker build -t stepwise:$(VERSION) .
