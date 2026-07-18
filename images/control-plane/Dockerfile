FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY sandboxd/go.mod sandboxd/go.sum sandboxd/
RUN go mod download

COPY api/ api/
COPY cmd/control-plane/ cmd/control-plane/
COPY internal/controlplane/ internal/controlplane/
COPY sandboxd/gen/ sandboxd/gen/
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/control-plane /control-plane
USER 65532:65532
ENTRYPOINT ["/control-plane"]
