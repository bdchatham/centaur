# A standard test interface for automated CI: an external oracle, or a
# person on a Mac or Linux, runs these targets the same way.
#
#   make test-list                    the test ids, one per line
#   make test [TESTS='<id> …']        run them (all when TESTS is empty), with
#                                     JUnit results at $(TEST_RESULTS)
#   make check                        the gates: api-rs's fmt and clippy
#
# They cover api-rs's packages that the swarm works in, through
# cargo-nextest, one test at a time (their database tests share schemas). An
# id is `<nextest binary id>/<test name>`, unique across the test binaries.
# DATABASE_URL, when set (a CI runner gives each run a fresh database), is the
# database the session and workflow tests use.

SHELL := /bin/bash

WORKSPACE := services/api-rs
PACKAGES := centaur-workflows centaur-session-core centaur-session-runtime centaur-session-sqlx centaur-api-server
TESTS ?=
TEST_RESULTS ?= $(CURDIR)/$(WORKSPACE)/target/test-results.xml
# Tools installed once: a CI runner's persistent TOOLS_DIR, else ./.tools.
TOOLS_DIR ?= $(CURDIR)/.tools

ifdef DATABASE_URL
export SESSION_RUNTIME_TEST_DATABASE_URL := $(DATABASE_URL)
export SESSION_SQLX_TEST_DATABASE_URL := $(DATABASE_URL)
export ABSURD_TEST_DATABASE_URL := $(DATABASE_URL)
endif

# cargo-nextest, prebuilt (never compiled here), pinned by version and by
# each release tarball's sha256 (github.com/nextest-rs/nextest/releases).
# Only the tarball is kept in TOOLS_DIR; see NEXTEST_FRESH.
NEXTEST_VERSION := 0.9.146
NEXTEST_TARGET_Darwin := universal-apple-darwin
NEXTEST_TARGET_Linux_aarch64 := aarch64-unknown-linux-gnu
NEXTEST_TARGET_Linux_arm64 := aarch64-unknown-linux-gnu
NEXTEST_TARGET_Linux_x86_64 := x86_64-unknown-linux-gnu
NEXTEST_SHA256_universal-apple-darwin := 39785160b3c2f6ed9a765049cf4fa79f3b39aa02eb7598a5a0e2a1a0b9ffb9a8
NEXTEST_SHA256_aarch64-unknown-linux-gnu := b2e33d7c72de7ade0ff7b3a948ac37516b24f8a836b7a8870c1f634a94be9de9
NEXTEST_SHA256_x86_64-unknown-linux-gnu := 682c21b777c333e96fd532e114d3a5a894e0729ab88d94c0a9f20f8419695428

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
NEXTEST_TARGET := $(or $(NEXTEST_TARGET_$(UNAME_S)),$(NEXTEST_TARGET_$(UNAME_S)_$(UNAME_M)))
NEXTEST_SHA256 := $(NEXTEST_SHA256_$(NEXTEST_TARGET))
NEXTEST_URL := https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-$(NEXTEST_VERSION)/cargo-nextest-$(NEXTEST_VERSION)-$(NEXTEST_TARGET).tar.gz
NEXTEST_TARBALL := $(TOOLS_DIR)/cargo-nextest-$(NEXTEST_VERSION)-$(NEXTEST_TARGET).tar.gz
SHA256 := $(shell command -v sha256sum >/dev/null 2>&1 && echo sha256sum || echo 'shasum -a 256')

# cargo-nextest for one recipe, at $$nextest. TOOLS_DIR outlives the code
# under test, which could rewrite what it holds, so it keeps only the
# tarball: each use copies it, checks the copy against the pinned sha256
# (downloading it again when it doesn't match), and extracts it into a
# temp dir removed on exit. Messages go to stderr: test-list's stdout is ids.
NEXTEST_FRESH = [ -n "$(NEXTEST_SHA256)" ] || { echo "no pinned cargo-nextest build for $(UNAME_S) $(UNAME_M)" >&2; exit 1; }; \
	tmp=$$(mktemp -d "$${TMPDIR:-/tmp}/cargo-nextest.XXXXXX") || exit 1; trap 'rm -rf "$$tmp"' EXIT; \
	pinned() { sum=$$($(SHA256) < "$$1") && [ "$${sum%% *}" = "$(NEXTEST_SHA256)" ]; }; \
	if ! { cp "$(NEXTEST_TARBALL)" "$$tmp/nextest.tar.gz" 2>/dev/null && pinned "$$tmp/nextest.tar.gz"; }; then \
	  echo "downloading cargo-nextest $(NEXTEST_VERSION) ($(NEXTEST_TARGET)) into $(TOOLS_DIR)" >&2; \
	  curl -fsSL --retry 3 -o "$$tmp/nextest.tar.gz" "$(NEXTEST_URL)" || exit 1; \
	  pinned "$$tmp/nextest.tar.gz" || { echo "cargo-nextest's download doesn't match its pinned sha256" >&2; exit 1; }; \
	  mkdir -p "$(TOOLS_DIR)" && cp "$$tmp/nextest.tar.gz" "$(NEXTEST_TARBALL).$$$$" && \
	    mv "$(NEXTEST_TARBALL).$$$$" "$(NEXTEST_TARBALL)" || exit 1; \
	fi; \
	tar -xzf "$$tmp/nextest.tar.gz" -C "$$tmp" cargo-nextest || exit 1; \
	nextest="$$tmp/cargo-nextest"

# nextest's settings come from here, not from a commit's .config/nextest.toml:
# no fail-fast, no retries, one test at a time, JUnit in its store
# ($(WORKSPACE)/target/nextest/ci-make/).
NEXTEST_PROFILE := ci-make
NEXTEST_CONFIG := $(CURDIR)/$(WORKSPACE)/target/$(NEXTEST_PROFILE)/nextest.toml
NEXTEST_JUNIT := $(CURDIR)/$(WORKSPACE)/target/nextest/$(NEXTEST_PROFILE)/junit.xml
NEXTEST_ARGS := --locked --manifest-path $(WORKSPACE)/Cargo.toml $(addprefix -p ,$(PACKAGES)) --config-file $(NEXTEST_CONFIG) --profile $(NEXTEST_PROFILE)

.PHONY: test-list test check nextest-config

test-list: nextest-config
	@$(NEXTEST_FRESH); set -o pipefail; "$$nextest" nextest list $(NEXTEST_ARGS) --message-format oneline | sed 's| |/|'

# Each id selects its binary and its exact name. JUnit names a test by its
# name, with its binary as classname: the results rename it to its id.
test: nextest-config
	@$(NEXTEST_FRESH); \
	filters=(); \
	for id in $(TESTS); do filters+=(-E "binary_id(=$${id%/*}) & test(=$${id##*/})"); done; \
	rm -f "$(NEXTEST_JUNIT)"; \
	"$$nextest" nextest run $(NEXTEST_ARGS) --no-fail-fast --no-tests=warn $${filters[@]+"$${filters[@]}"}; \
	status=$$?; \
	if [ -f "$(NEXTEST_JUNIT)" ]; then \
	  mkdir -p "$$(dirname "$(TEST_RESULTS)")" && \
	  sed 's|<testcase name="\([^"]*\)" classname="\([^"]*\)"|<testcase name="\2/\1" classname="\2"|' \
	    "$(NEXTEST_JUNIT)" > "$(TEST_RESULTS).partial" && \
	  mv "$(TEST_RESULTS).partial" "$(TEST_RESULTS)"; \
	fi; \
	exit $$status

check:
	cd $(WORKSPACE) && cargo fmt --all --check
	cd $(WORKSPACE) && cargo clippy --locked --all-targets -- -D warnings

nextest-config:
	@mkdir -p "$(dir $(NEXTEST_CONFIG))"
	@printf '%s\n' '[store]' 'dir = "target/nextest"' '[profile.$(NEXTEST_PROFILE)]' 'fail-fast = false' \
	  'retries = 0' 'test-threads = 1' '[profile.$(NEXTEST_PROFILE).junit]' 'path = "junit.xml"' > "$(NEXTEST_CONFIG)"
