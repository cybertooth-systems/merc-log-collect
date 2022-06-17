# syntax = docker/dockerfile:1-experimental

FROM --platform=${BUILDPLATFORM} golang:1.18-bullseye AS base

WORKDIR /src
COPY go.* db-migration.sql .
RUN go mod download

FROM base AS build
ENV CGO_ENABLED=1
ARG TARGETOS
ARG TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /out/merc-log-collect .

FROM scratch AS bin-unix
COPY --from=build /out/merc-log-collect /

FROM bin-unix AS bin-linux
FROM bin-unix AS bin-darwin

FROM scratch AS bin-windows
COPY --from=build /out/merc-log-collect /merc-log-collect.exe

FROM bin-${TARGETOS} AS bin
