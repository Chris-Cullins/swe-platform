FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY sandboxd/go.mod sandboxd/go.sum ./
RUN go mod download

COPY sandboxd/ ./
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/sandboxd ./cmd/sandboxd

FROM build AS terminal-test-build
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go test -c -o /out/server.test ./internal/server

FROM debian:12-slim AS tmux-build

ARG TMUX_VERSION=3.3a
RUN apt-get update && apt-get install -y --no-install-recommends \
		build-essential ca-certificates curl libevent-dev libncurses-dev patch pkg-config \
	&& rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN curl -fsSL "https://github.com/tmux/tmux/releases/download/${TMUX_VERSION}/tmux-${TMUX_VERSION}.tar.gz" -o tmux.tar.gz \
	&& echo "e4fd347843bd0772c4f48d6dde625b0b109b7a380ff15db21e97c11a4dcdf93f  tmux.tar.gz" | sha256sum -c - \
	&& tar -xzf tmux.tar.gz --strip-components=1
COPY images/env-base/tmux-control-output-drain.patch ./
RUN patch -p1 <tmux-control-output-drain.patch \
	&& ./configure \
	&& sed -i '/DTMUX_VERSION/s/3\.3a/3.3a-swe-drain/' Makefile \
	&& make -j"$(nproc)" \
	&& test "$(./tmux -V)" = "tmux 3.3a-swe-drain"

FROM node:24-bookworm-slim AS claude-code
ARG CLAUDE_CODE_VERSION=2.1.215
RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}"

# Base image for environments.
FROM debian:12-slim AS environment

RUN apt-get update && apt-get install -y --no-install-recommends \
		git ca-certificates curl tmux jq ripgrep \
	&& rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 swe \
	&& mkdir -p /workspace \
	&& chown swe:swe /workspace

COPY --from=build /out/sandboxd /usr/local/bin/sandboxd
COPY --from=tmux-build /src/tmux /usr/bin/tmux
COPY --from=claude-code /usr/local/lib/node_modules/@anthropic-ai /usr/local/lib/node_modules/@anthropic-ai
COPY --from=claude-code /usr/local/bin/claude /usr/local/bin/claude
RUN claude --version

USER swe
WORKDIR /workspace

# sandboxd is the environment's entrypoint: it is the single contract the
# control plane uses to reach inside (exec / fs / terminal / ports / health).
ENTRYPOINT ["sandboxd", "serve"]

FROM environment AS terminal-test
COPY --from=terminal-test-build /out/server.test /usr/local/bin/server.test
ENTRYPOINT ["/usr/local/bin/server.test"]

FROM environment AS release
