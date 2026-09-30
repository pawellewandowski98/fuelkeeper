GO ?= go
COMPOSE ?= docker compose
PKG := ./...
PG_DSN ?= postgres://fuelkeeper:fuelkeeper@localhost:15432/fuelkeeper?sslmode=disable

# bench-run defaults: a three-instance cluster (bench-cluster-up), 64
# workers, one output per reserve, 60s, against the postgres-backed cluster.
# OUT defaults to /tmp (not docs/benchmarks/, which does not exist in this
# tree and whose creation is out of scope here).
WORKERS ?= 64
COUNT ?= 1
DURATION ?= 60s
LABEL ?= cluster
OUT ?= /tmp/fuelbench-last.json

.PHONY: build test test-integration lint bench docker-up docker-down tidy bench-cluster-up bench-cluster-down bench-run

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

bench-cluster-up:
	$(COMPOSE) -f deploy/docker-compose.bench.yaml up -d --build --wait

bench-cluster-down:
	$(COMPOSE) -f deploy/docker-compose.bench.yaml down -v

bench-run:
	$(GO) run ./cmd/fuelbench --url http://localhost:8081,http://localhost:8082,http://localhost:8083 --workers $(WORKERS) --count $(COUNT) --duration $(DURATION) --label "$(LABEL)" --backend postgres --out $(OUT)
