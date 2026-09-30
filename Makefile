GO ?= go
COMPOSE ?= docker compose
PKG := ./...
PG_DSN ?= postgres://fuelkeeper:fuelkeeper@localhost:15432/fuelkeeper?sslmode=disable

.PHONY: build test test-integration lint bench docker-up docker-down tidy

build:
	$(GO) build $(PKG)

test:
	$(GO) test -race -count=1 $(PKG)

test-integration:
	FUELKEEPER_TEST_POSTGRES_DSN="$(PG_DSN)" $(GO) test -race -count=1 -parallel=8 -tags=postgres ./pkg/store/postgres/...

lint:
	golangci-lint run

bench:
	$(GO) test -run='^$$' -bench=. -benchmem ./pkg/store/...

docker-up:
	$(COMPOSE) -f deploy/docker-compose.yaml up -d --wait

docker-down:
	$(COMPOSE) -f deploy/docker-compose.yaml down -v

tidy:
	$(GO) mod tidy
