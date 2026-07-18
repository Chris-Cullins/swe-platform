# Base image for environments. Build context must contain a linux/amd64
# sandboxd binary (see `make docker-build-env-base`).
FROM debian:12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
		git ca-certificates curl tmux jq ripgrep \
	&& rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 swe \
	&& mkdir -p /workspace \
	&& chown swe:swe /workspace

COPY sandboxd /usr/local/bin/sandboxd

USER swe
WORKDIR /workspace

# sandboxd is the environment's entrypoint: it is the single contract the
# control plane uses to reach inside (exec / fs / terminal / ports / health).
ENTRYPOINT ["sandboxd", "serve"]
