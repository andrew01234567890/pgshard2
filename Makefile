.PHONY: all build test lint fmt clean

all: build

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
