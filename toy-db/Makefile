# Makefile for ToyDB

# Default Go commands
GO := go
BINARY := toydb

# Directories
SRC_DIR := ./internal
TEST_DIR := ./internal/...

# Go build flags
BUILD_FLAGS := -v

# Detect OS
UNAME := $(shell uname)

# ----------------------------
# Build targets
# ----------------------------

.PHONY: all linux darwin clean test-linux test-darwin test

all: build

build:
	@echo "Building for current OS..."
	$(GO) build $(BUILD_FLAGS) -o $(BINARY) ./cmd/toydb

linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 $(GO) build $(BUILD_FLAGS) -o $(BINARY)_linux ./cmd/toydb

darwin:
	@echo "Building for macOS... $(UNAME)"
	GOOS=darwin GOARCH=amd64 $(GO) build $(BUILD_FLAGS) -o $(BINARY)_darwin ./cmd/toydb

# ----------------------------
# Test targets
# ----------------------------

test: test-linux test-darwin

test-linux:
	@echo "Running Linux tests..."
	GOOS=linux GOARCH=amd64 $(GO) test -tags=linux -v $(TEST_DIR)

test-darwin:
	@echo "Running Darwin tests..."
	GOOS=darwin GOARCH=amd64 $(GO) test -tags=darwin -v $(TEST_DIR)

# ----------------------------
# Clean
# ----------------------------
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(BINARY) $(BINARY)_linux $(BINARY)_darwin
