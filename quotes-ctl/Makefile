NAME := quotes-ctl
BUILD_DIR := build

LINUX_TARGET := x86_64-unknown-linux-musl
MACOS_ARM_TARGET := aarch64-apple-darwin

.PHONY: build build-linux build-macos-arm clean

build: build-linux

build-linux:
	cargo zigbuild --release --target $(LINUX_TARGET)
	mkdir -p $(BUILD_DIR)
	cp target/$(LINUX_TARGET)/release/$(NAME) $(BUILD_DIR)/$(NAME)-linux-amd64

build-macos-arm:
	cargo zigbuild --release --target $(MACOS_ARM_TARGET)
	mkdir -p $(BUILD_DIR)
	cp target/$(MACOS_ARM_TARGET)/release/$(NAME) $(BUILD_DIR)/$(NAME)-darwin-arm64

build-all: build-linux build-macos-arm

clean:
	cargo clean
	rm -rf $(BUILD_DIR)
