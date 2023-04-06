.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: setup
setup:
	cargo upgrade --pinned
	cargo update

.PHONY: docs
docs:
	cargo doc --open --no-deps --package surrealdb --features rustls,native-tls,protocol-ws,protocol-http,storage-mem,storage-indxdb,storage-rocksdb,storage-tikv,http,scripting

.PHONY: test
test:
	cargo test --workspace

.PHONY: check
check:
	cargo check --workspace
	cargo fmt --all -- --check
	cargo clippy -- -W warnings

.PHONY: clean
clean:
	cargo clean

.PHONY: serve
serve:
	cargo run -- start --log trace --user root --pass root memory

.PHONY: cluster
cluster:
	SURREAL_TRACING_TRACER=otlp cargo run --features storage-tikv -- start --log trace --user root --pass root tikv://localhost:2379 &\
	SURREAL_TRACING_TRACER=otlp cargo run --features storage-tikv -- start --log trace --user root --pass root -b 0.0.0.0:8001 tikv://localhost:2379

.PHONE: cluster-tikv
cluster-tikv:
	docker-compose up -d

.PHONY: sql
sql:
	cargo run -- sql --conn ws://0.0.0.0:8000 --user root --pass root --ns test --db test --pretty

.PHONY: quick
quick:
	cargo build

.PHONY: build
build:
	cargo build --release
