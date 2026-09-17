# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/wager ./cmd/wager

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 wager
USER wager
COPY --from=build /out/wager /usr/local/bin/wager
EXPOSE 8081
ENTRYPOINT ["wager"]
