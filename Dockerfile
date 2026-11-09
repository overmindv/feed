FROM golang:1.26-alpine AS build
ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
ARG GO_BUILD_PARALLEL=2
ENV GOFLAGS=-p=${GO_BUILD_PARALLEL}
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,id=overmindv-gomod,target=/go/pkg/mod \
    --mount=type=cache,id=overmindv-gobuild,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/feed ./cmd/feed

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget && addgroup -S feed && adduser -S feed -G feed
WORKDIR /app
COPY --from=build /out/feed /usr/local/bin/feed
COPY migrations /app/migrations
USER feed
EXPOSE 8080
ENTRYPOINT ["feed"]
