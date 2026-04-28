# syntax=docker/dockerfile:1

# ---- builder ----
FROM golang:1.25-alpine AS builder
WORKDIR /build

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/twinval-api ./cmd/api

# ---- runtime ----
FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/twinval-api /app/twinval-api

EXPOSE 8080
ENV TWINVAL_PORT=8080

ENTRYPOINT ["/app/twinval-api"]
