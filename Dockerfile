# syntax=docker/dockerfile:1

FROM golang:1.26.6-alpine AS builder

WORKDIR /src

COPY go.mod go.sum go.work go.work.sum ./
COPY cmd/spool-rack/go.mod cmd/spool-rack/go.sum ./cmd/spool-rack/
RUN go mod download && cd cmd/spool-rack && go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/spool-rack ./cmd/spool-rack

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 spoolrack \
    && adduser -S -D -H -u 10001 -G spoolrack spoolrack \
    && mkdir -p /var/lib/spool-rack \
    && chown spoolrack:spoolrack /var/lib/spool-rack

COPY --from=builder /out/spool-rack /usr/local/bin/spool-rack

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/spool-rack"]
