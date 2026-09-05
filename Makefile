.PHONY: all build test test-race test-postgres lint tidy tidy-check fmt fmt-check clean db-up db-down

all: tidy-check lint test test-race build

build:
	mkdir -p bin
	go build ./...
	go build -o bin/spool-rack ./cmd/spool-rack

test:
	go test -v ./...
	go test -v ./cmd/spool-rack/...

test-race:
	go test -v -race ./...
	go test -v -race ./cmd/spool-rack/...

test-postgres: db-up
	go test -v -race ./internal/server/storage/postgres/...

lint: fmt-check
	golangci-lint run ./...
	golangci-lint run ./cmd/spool-rack/...

tidy:
	go work sync
	go mod tidy
	cd cmd/spool-rack && go mod tidy

tidy-check:
	go mod tidy -diff
	cd cmd/spool-rack && go mod tidy -diff

fmt:
	gofmt -l -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files found: run make fmt" && exit 1)

clean:
	rm -rf bin dist coverage.out

db-up:
	docker compose up -d postgres

db-down:
	docker compose down
