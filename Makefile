.PHONY: run build test lint cover docker

run:
	go run ./cmd/tollgate

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/tollgate ./cmd/tollgate

test:
	go test -race -cover ./...

cover:
	go test -race -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run gofmt -w ." && exit 1)
	go vet ./...

docker:
	docker build -t tollgate:dev .
