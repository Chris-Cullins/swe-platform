FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/control-plane/ cmd/control-plane/
COPY internal/controlplane/ internal/controlplane/
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/control-plane /control-plane
USER 65532:65532
ENTRYPOINT ["/control-plane"]
