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

FROM node:24-bookworm-slim AS amp
ARG AMP_VERSION=0.0.1784492094-g5d18e2
ARG TARGETARCH
RUN case "$TARGETARCH" in \
		amd64) package=cli-linux-x64; integrity='sha512-63OLnzbYy7iZhHcw1TaEaTOcdr18XouL2RtxLTH9T8BBGOvnFVMxBTHQzl3H4sQj2X5NVnxVEsLFvODBfJD4yg==' ;; \
		arm64) package=cli-linux-arm64; integrity='sha512-URIOwVZ6XI1fe/iH0xTAVi7KQ16xUKFlyF++fv4x8aCNKQhhR4v01RuAs4ePp8Trqt3vckLkaAPmYQY0eMkUaw==' ;; \
		*) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
	esac \
	&& wrapper="$(npm pack "@ampcode/cli@${AMP_VERSION}" --silent)" \
	&& node -e 'const [file, want] = process.argv.slice(1); const fs = require("fs"), crypto = require("crypto"); const got = "sha512-" + crypto.createHash("sha512").update(fs.readFileSync(file)).digest("base64"); if (got !== want) { throw new Error(`integrity mismatch for ${file}`) }' "$wrapper" 'sha512-uLTXtnFeA5ZNHe55NFuvFUS9q3o46hM8njiIF7CInzlrT+xhWnR6RcoFTjCqF6ZtXbJl2anQeK2LDpJpN1UvbQ==' \
	&& archive="$(npm pack "@ampcode/${package}@${AMP_VERSION}" --silent)" \
	&& node -e 'const [file, want] = process.argv.slice(1); const fs = require("fs"), crypto = require("crypto"); const got = "sha512-" + crypto.createHash("sha512").update(fs.readFileSync(file)).digest("base64"); if (got !== want) { throw new Error(`integrity mismatch for ${file}`) }' "$archive" "$integrity" \
	&& tar -xzf "$archive" package/amp --strip-components=1 \
	&& install -m 0755 amp /usr/local/bin/amp \
	&& rm "$wrapper" "$archive" amp \
	&& test "$(/usr/local/bin/amp --version | awk '{print $1}')" = "${AMP_VERSION}"

FROM node:24-bookworm-slim AS codex
ARG CODEX_VERSION=0.144.6
ARG TARGETARCH
RUN case "$TARGETARCH" in \
		amd64) suffix=linux-x64; triple=x86_64-unknown-linux-musl; integrity='sha512-4E7EnzCg0OnBxCyYnwJ+qnZwWHYe0YScr5ucKWbngE9u4+0XrpWELqq2Kn9jl5GZK8MDjU7PrJwFIwusHOHjuw==' ;; \
		arm64) suffix=linux-arm64; triple=aarch64-unknown-linux-musl; integrity='sha512-PGiLXMN+2IQRkf7tOLi64dMInjU1pRLbz0Rwfj/yt2Y97SZQqAjFQoi2wmswmqtqMDnfwCPTC1DRXVQkvU6T6Q==' ;; \
		*) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
	esac \
	&& wrapper="$(npm pack "@openai/codex@${CODEX_VERSION}" --silent)" \
	&& archive="$(npm pack "@openai/codex@${CODEX_VERSION}-${suffix}" --silent)" \
	&& node -e 'const [file, want] = process.argv.slice(1); const fs = require("fs"), crypto = require("crypto"); const got = "sha512-" + crypto.createHash("sha512").update(fs.readFileSync(file)).digest("base64"); if (got !== want) throw new Error(`integrity mismatch for ${file}`)' "$wrapper" 'sha512-wk+2CWiBNXiJLBoN2D08N9RceWkSBnlgk5g2K1a4CXrP/C0gdlHyRUG7RFzm9y41DCK/7tvCct233JVxyFmznw==' \
	&& node -e 'const [file, want] = process.argv.slice(1); const fs = require("fs"), crypto = require("crypto"); const got = "sha512-" + crypto.createHash("sha512").update(fs.readFileSync(file)).digest("base64"); if (got !== want) throw new Error(`integrity mismatch for ${file}`)' "$archive" "$integrity" \
	&& mkdir -p /opt/codex && tar -xzf "$archive" -C /opt/codex --strip-components=3 "package/vendor/$triple" \
	&& test -x /opt/codex/codex-resources/bwrap \
	&& test -f /opt/codex/codex-package.json \
	&& test "$(/opt/codex/bin/codex --version)" = "codex-cli ${CODEX_VERSION}"

FROM node:24-bookworm-slim AS pi
ARG PI_CODING_AGENT_VERSION=0.80.10
ARG PI_CODING_AGENT_INTEGRITY=sha512-aL4apbupCHiVLSXASXvRzH4Q2vmtfrDa+0s909CJuVu/GgGylbDzr7oyF1mPmip5E+VxYYxKWmph4hV04wUcQg==
WORKDIR /tmp/pi
RUN npm pack "@earendil-works/pi-coding-agent@${PI_CODING_AGENT_VERSION}" --json > pack.json \
	&& test "$(node -p 'JSON.parse(require("fs").readFileSync("pack.json"))[0].integrity')" = "${PI_CODING_AGENT_INTEGRITY}" \
	&& npm install -g --ignore-scripts "./$(node -p 'JSON.parse(require("fs").readFileSync("pack.json"))[0].filename')" \
	&& test "$(pi --version)" = "${PI_CODING_AGENT_VERSION}"

# Base image for environments.
FROM node:24-bookworm-slim AS environment

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
COPY --from=amp /usr/local/bin/amp /usr/local/bin/amp
COPY --from=codex /opt/codex /opt/codex
COPY --from=pi /usr/local/lib/node_modules/@earendil-works /usr/local/lib/node_modules/@earendil-works
COPY --from=pi /usr/local/bin/pi /usr/local/bin/pi
RUN ln -s /opt/codex/bin/codex /usr/local/bin/codex
ENV AMP_SKIP_UPDATE_CHECK=1
RUN claude --version \
	&& test "$(amp --version | awk '{print $1}')" = "0.0.1784492094-g5d18e2" \
	&& test "$(codex --version)" = "codex-cli 0.144.6" \
	&& test "$(readlink -f "$(command -v codex)")" = /opt/codex/bin/codex \
	&& test -x /opt/codex/codex-resources/bwrap \
	&& test -f /opt/codex/codex-package.json \
	&& test "$(pi --version)" = "0.80.10"

USER swe
WORKDIR /workspace

# sandboxd is the environment's entrypoint: it is the single contract the
# control plane uses to reach inside (exec / fs / terminal / ports / health).
ENTRYPOINT ["sandboxd", "serve"]

FROM environment AS terminal-test
COPY --from=terminal-test-build /out/server.test /usr/local/bin/server.test
ENTRYPOINT ["/usr/local/bin/server.test"]

FROM environment AS release
