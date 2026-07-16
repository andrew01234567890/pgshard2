.PHONY: all build test lint fmt clean proto

PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

all: build

proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	buf lint
	buf generate

build:
	cargo build --workspace
	$(MAKE) -C operator build

test:
	cargo test --workspace
	$(MAKE) -C operator test

lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	$(MAKE) -C operator lint

fmt:
	cargo fmt --all
	$(MAKE) -C operator fmt

clean:
	cargo clean
	rm -rf operator/bin

.PHONY: kind-up kind-down e2e

kind-up:
	$(MAKE) -C operator setup-test-e2e
	kubectl --context kind-pgshard-test-e2e apply -f test/e2e/manifests/minio.yaml
	kubectl --context kind-pgshard-test-e2e -n pgshard-e2e rollout status deployment/minio --timeout=180s

kind-down:
	$(MAKE) -C operator cleanup-test-e2e

e2e: kind-up
	$(MAKE) -C operator test-e2e
