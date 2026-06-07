FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/node ./cmd/node
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/node /usr/local/bin/node
COPY --from=builder /out/healthcheck /usr/local/bin/healthcheck

EXPOSE 6379 6060

ENV CACHE_DEBUG_ADDR=:6060
ENV HEALTHCHECK_URL=http://127.0.0.1:6060/healthz

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/healthcheck"]

ENTRYPOINT ["/usr/local/bin/node"]
