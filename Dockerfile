# syntax=docker/dockerfile:1

# ── build stage ───────────────────────────────────────────────────────────────
# Pinned to go.mod's toolchain. --platform=$BUILDPLATFORM keeps the compiler running
# on the runner's native arch; Go then cross-compiles to $TARGETARCH (fast, no QEMU
# emulation of the build itself).
FROM --platform=$BUILDPLATFORM golang:1.25.7 AS build

# Injected by buildx for each --platform value.
ARG TARGETOS
ARG TARGETARCH

# Build metadata, stamped into the binary via -ldflags.
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /src

# Dependencies first: this layer is cached unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Source.
COPY . .

# Static binary for the target platform, with BuildKit caches for modules + compiler.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/pulsed ./cmd/pulsed

# ── runtime stage ─────────────────────────────────────────────────────────────
# distroless static: no shell, no package manager, non-root by default; ships CA
# certificates + tzdata, so outbound TLS (DB / future gRPC TLS) works.
FROM gcr.io/distroless/static:nonroot AS runtime

COPY --from=build /out/pulsed /pulsed

EXPOSE 50051
USER nonroot:nonroot
ENTRYPOINT ["/pulsed"]
