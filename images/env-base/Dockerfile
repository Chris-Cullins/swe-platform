FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY sandboxd/go.mod sandboxd/go.sum ./
RUN go mod download

COPY sandboxd/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/sandboxd ./cmd/sandboxd

# Base image for environments.
FROM debian:12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
		git ca-certificates curl tmux jq ripgrep \
	&& rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 swe \
	&& mkdir -p /workspace \
	&& chown swe:swe /workspace

COPY --from=build /out/sandboxd /usr/local/bin/sandboxd

USER swe
WORKDIR /workspace

# sandboxd is the environment's entrypoint: it is the single contract the
# control plane uses to reach inside (exec / fs / terminal / ports / health).
ENTRYPOINT ["sandboxd", "serve"]
