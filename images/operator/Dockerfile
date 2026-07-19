# Build the operator binary.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY sandboxd/go.mod sandboxd/go.sum sandboxd/
RUN go mod download

COPY api/ api/
COPY cmd/operator/ cmd/operator/
COPY internal/ internal/
COPY sandboxd/auth/ sandboxd/auth/
COPY sandboxd/gen/proto/sandboxd/v1/ sandboxd/gen/proto/sandboxd/v1/
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/operator ./cmd/operator

# Run it.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/operator /operator
USER 65532:65532
ENTRYPOINT ["/operator"]
