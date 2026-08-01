# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

FROM --platform=$BUILDPLATFORM node:22.21.0-alpine@sha256:bd26af08779f746650d95a2e4d653b0fd3c8030c44284b6b98d701c9b5eb66b9 AS web

WORKDIR /src/webmanager

COPY webmanager/package.json webmanager/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY webmanager/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.25.3-alpine@sha256:aee43c3ccbf24fdffb7295693b6e33b21e01baec1b2a55acc351fde345e9ec34 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY --from=web /src/internal/status/webmanager/dist ./internal/status/webmanager/dist

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/via ./cmd/via

FROM scratch

LABEL org.opencontainers.image.title="Via" \
      org.opencontainers.image.description="Bounded multipath TCP relay" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build /out/via /via

USER 65532:65532
EXPOSE 1080 38473 9090 9091
STOPSIGNAL SIGTERM
ENTRYPOINT ["/via"]
CMD ["version"]
