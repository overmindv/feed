FROM golang:1.26-alpine AS build
ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}
WORKDIR /src
# parker подтягивается по тегу (v0.1.0) из модульного прокси — см. go.mod / go.sum.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/feed ./cmd/feed

FROM alpine:3.22
RUN apk add --no-cache ca-certificates wget && addgroup -S feed && adduser -S feed -G feed
WORKDIR /app
COPY --from=build /out/feed /usr/local/bin/feed
COPY migrations /app/migrations
USER feed
EXPOSE 8080
ENTRYPOINT ["feed"]
