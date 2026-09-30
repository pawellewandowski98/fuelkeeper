# Dockerfile builds fuelkeeper and fuelbench as static (CGO_ENABLED=0)
# binaries. The store's sqlite backend uses modernc.org/sqlite (pure Go, no
# cgo); mattn/go-sqlite3 appears in go.sum only as an unused transitive
# dependency, so no C toolchain is required here. See docs/OPERATIONS.md
# "Benchmark cluster" for how this image is used.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fuelkeeper ./cmd/fuelkeeper && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fuelbench ./cmd/fuelbench

FROM alpine:3.20
RUN adduser -D fuel
COPY --from=build /out/fuelkeeper /out/fuelbench /usr/local/bin/
USER fuel
EXPOSE 8080
ENTRYPOINT ["fuelkeeper"]
CMD ["serve", "--config", "/etc/fuelkeeper/fuelkeeper.yaml"]
