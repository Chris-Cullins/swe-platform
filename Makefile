# swe-platform — root Makefile.
# Two Go modules live here: the root module (operator + CLI + API types)
# and sandboxd/ (the in-environment daemon, kept dependency-light).

CONTROLLER_GEN_VERSION ?= v0.21.0
PROTOC_GEN_GO_VERSION ?= latest
PROTOC_GEN_GO_GRPC_VERSION ?= latest

LOCALBIN := $(abspath bin)
CONTROLLER_GEN := $(LOCALBIN)/controller-gen

KIND_CLUSTER ?= swe-dev

.PHONY: all
all: build

##@ Build

.PHONY: build
build: build-operator build-control-plane build-cli build-sandboxd ## Build all binaries into bin/
	$(MAKE) check-build-output

.PHONY: build-operator
build-operator: ## Build the operator
	go build -o $(LOCALBIN)/operator ./cmd/operator

.PHONY: build-control-plane
build-control-plane: ## Build the control-plane API
	go build -o $(LOCALBIN)/control-plane ./cmd/control-plane

.PHONY: build-cli
build-cli: ## Build the swe CLI
	go build -o $(LOCALBIN)/swe ./cmd/swe

.PHONY: build-sandboxd
build-sandboxd: ## Build sandboxd
	go build -C sandboxd -o $(LOCALBIN)/sandboxd ./cmd/sandboxd

##@ Test & verify

.PHONY: check-build-output
check-build-output: ## Verify all built binaries land in bin/
	@test -x "$(LOCALBIN)/operator"
	@test -x "$(LOCALBIN)/control-plane"
	@test -x "$(LOCALBIN)/swe"
	@test -x "$(LOCALBIN)/sandboxd"

.PHONY: test
test: ## Run unit tests in both modules
	go test ./...
	cd sandboxd && go test ./...

.PHONY: vet
vet: ## Run go vet in both modules
	go vet ./...
	cd sandboxd && go vet ./...

##@ Code generation

.PHONY: generate
generate: $(CONTROLLER_GEN) ## Generate deepcopy methods
	$(CONTROLLER_GEN) object paths="./api/..."

.PHONY: manifests sync-chart-crds check-chart-crds
manifests: $(CONTROLLER_GEN) ## Generate CRDs and RBAC into config/
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths="./internal/controllers/..." output:rbac:artifacts:config=config/rbac
	$(MAKE) sync-chart-crds

sync-chart-crds: ## Synchronize generated CRDs into the Helm chart
	mkdir -p charts/swe-platform/crds
	cp config/crd/bases/*.yaml charts/swe-platform/crds/

check-chart-crds: ## Verify Helm CRDs match the generated manifests
	diff -ru config/crd/bases charts/swe-platform/crds

.PHONY: proto
proto: ## Regenerate sandboxd protobuf code (requires protoc)
	GOBIN=$(LOCALBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(LOCALBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	mkdir -p sandboxd/gen
	protoc \
		--plugin=protoc-gen-go=$(LOCALBIN)/protoc-gen-go \
		--plugin=protoc-gen-go-grpc=$(LOCALBIN)/protoc-gen-go-grpc \
		--go_out=sandboxd/gen --go_opt=paths=source_relative \
		--go-grpc_out=sandboxd/gen --go-grpc_opt=paths=source_relative \
		--proto_path=sandboxd sandboxd/proto/sandboxd/v1/sandboxd.proto

$(CONTROLLER_GEN):
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

##@ Local cluster

.PHONY: kind-up
kind-up: ## Create the kind dev cluster
	./hack/kind-up.sh

.PHONY: kind-down
kind-down: ## Delete the kind dev cluster
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: argocd-up
argocd-up: ## Create the Argo CD cluster tracking origin/main
	./hack/argocd-up.sh

.PHONY: argocd-down
argocd-down: ## Delete the Argo CD cluster
	kind delete cluster --name swe-argo

.PHONY: install-crds
install-crds: manifests ## Install CRDs into the current cluster
	kubectl apply -f config/crd/bases

.PHONY: run
run: ## Run the operator locally against the current cluster
	go run ./cmd/operator

##@ Images

.PHONY: docker-build
docker-build: docker-build-operator docker-build-control-plane docker-build-env-base ## Build all images

.PHONY: docker-build-operator
docker-build-operator: ## Build the operator image
	docker build -t ghcr.io/chris-cullins/swe-platform/operator:dev -f images/operator/Dockerfile .

.PHONY: docker-build-control-plane
docker-build-control-plane: ## Build the control-plane image
	docker build -t ghcr.io/chris-cullins/swe-platform/control-plane:dev -f images/control-plane/Dockerfile .

.PHONY: docker-build-env-base
docker-build-env-base: ## Build the environment base image (includes sandboxd)
	docker build -t ghcr.io/chris-cullins/swe-platform/env-base:dev -f images/env-base/Dockerfile .

##@ Misc

.PHONY: clean
clean: ## Remove built binaries
	rm -rf $(LOCALBIN)

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
