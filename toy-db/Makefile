# Makefile for ToyDB

GO := go
BINARY := toydb

SRC_DIR := ./internal
TEST_DIR := ./internal/...

BUILD_FLAGS := -v

# Host detection
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Map uname -m to Go GOARCH values
ifeq ($(UNAME_M),x86_64)
    HOST_GOARCH := amd64
else ifeq ($(UNAME_M),arm64)
    HOST_GOARCH := arm64
else ifeq ($(UNAME_M),aarch64)
    HOST_GOARCH := arm64
else
    HOST_GOARCH := $(UNAME_M)
endif

.PHONY: all build linux darwin clean test test-native test-race test-stress-native test-linux test-darwin

all: build

# ----------------------------
# Build targets
# ----------------------------

build:
	@echo "Building for host ($(UNAME_S)/$(UNAME_M))..."
	$(GO) build $(BUILD_FLAGS) -o $(BINARY)

# Cross-compiled artifacts. Default GOARCH is amd64; override with GOARCH=arm64.
GOARCH_LINUX ?= amd64
GOARCH_DARWIN ?= amd64

linux:
	@echo "Cross-compiling for Linux/$(GOARCH_LINUX)..."
	GOOS=linux GOARCH=$(GOARCH_LINUX) $(GO) build $(BUILD_FLAGS) -o $(BINARY)_linux_$(GOARCH_LINUX)

darwin:
	@echo "Cross-compiling for Darwin/$(GOARCH_DARWIN)..."
	GOOS=darwin GOARCH=$(GOARCH_DARWIN) $(GO) build $(BUILD_FLAGS) -o $(BINARY)_darwin_$(GOARCH_DARWIN)

# ----------------------------
# Test targets
# ----------------------------

# Default: run tests on the host. This is the only target that works everywhere.
test:
	@echo "Running host tests..."
	$(GO) test -v $(TEST_DIR)

test-native:
	@echo "Running host tests..."
	$(GO) test -v $(TEST_DIR)

test-race:
	@echo "Running host tests with race detector..."
	$(GO) test -race -v $(TEST_DIR)

test-stress-native:
	@echo "Stress testing (race detector, 10 runs)..."
	$(GO) test -race -count=10 -v $(TEST_DIR)

# Platform-specific tests. Only useful on the matching host OS (or with emulation).
# These are NOT cross-compiled; they run natively on the build host.
test-linux:
	@if [ "$(UNAME_S)" != "Linux" ]; then \
		echo "test-linux requires a Linux host; current host is $(UNAME_S)"; \
		exit 1; \
	fi
	@echo "Running Linux tests..."
	$(GO) test -race -v $(TEST_DIR)

test-darwin:
	@if [ "$(UNAME_S)" != "Darwin" ]; then \
		echo "test-darwin requires a Darwin host; current host is $(UNAME_S)"; \
		exit 1; \
	fi
	@echo "Running Darwin tests on $(UNAME_M)..."
	$(GO) test -race -v $(TEST_DIR)

# ----------------------------
# Clean
# ----------------------------

clean:
	@echo "Cleaning build artifacts..."
	rm -f $(BINARY) $(BINARY)_linux_* $(BINARY)_darwin_*