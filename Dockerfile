FROM --platform=${BUILDPLATFORM} golang:1.18-stretch AS build
WORKDIR /src
ENV CGO_ENABLED=1
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /out/merc-log-collect .

FROM scratch AS bin-unix
COPY --from=build /out/merc-log-collect /

FROM bin-unix AS bin-linux
FROM bin-unix AS bin-darwin

FROM scratch AS bin-windows
COPY --from=build /out/merc-log-collect /merc-log-collect.exe

FROM bin-${TARGETOS} AS bin
