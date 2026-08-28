#!/bin/bash
# Exit immediately if any command fails
set -e

# Usage:
#   ./deploy.sh                                build here with cross, then deploy
#   ./deploy.sh --release                      deploy the newest published release
#   ./deploy.sh --release 2026-08-16-a1b2c3d   deploy that exact release
#
# The Pi Zero is armv6 with a hard-float ABI, and linking against musl statically
# means the binary carries no glibc version coupling to whatever Raspberry Pi OS
# release is on the SD card. `cross` builds it inside a container that already
# has the matching toolchain, so no linker configuration is needed on this
# machine — which is why there is no .cargo/config.toml.
#
# PI is the ssh target for the Pi; override it for your own host and user, e.g.
#   PI=pi@bike.local ./deploy.sh
# The binary is placed in that user's home directory as m3i-ha-bridge-static,
# which is where install-service.sh expects it.
TARGET=arm-unknown-linux-musleabihf
PI="${PI:-pi@m3i-bridge.local}"
BINARY_NAME=m3i-ha-bridge-static

if [ "${1:-}" = "--release" ]; then
  TAG="${2:-}"
  DOWNLOAD_DIR="$(mktemp -d)"
  trap 'rm -rf "$DOWNLOAD_DIR"' EXIT

  echo "=== 1. Downloading release ${TAG:-(latest)} ==="
  # Only this target's binary and checksum: the release also carries builds for
  # other architectures.
  if [ -n "$TAG" ]; then
    gh release download "$TAG" --dir "$DOWNLOAD_DIR" --pattern "m3i-ha-bridge-*-$TARGET*" --clobber
  else
    gh release download --dir "$DOWNLOAD_DIR" --pattern "m3i-ha-bridge-*-$TARGET*" --clobber
  fi

  echo "=== 1b. Verifying checksum ==="
  # Downloaded under their published names rather than renamed, because the
  # .sha256 file's filename column has to match. `shasum -a 256 -c` reads
  # sha256sum's format and exists on macOS, where sha256sum does not.
  (cd "$DOWNLOAD_DIR" && shasum -a 256 -c ./*.sha256)

  BINARY="$(ls "$DOWNLOAD_DIR"/m3i-ha-bridge-*-"$TARGET")"
else
  echo "=== 1. Cross-compiling static MUSL ARMv6 binary ==="
  # --platform is needed only because this dev machine is arm64 macOS and the
  # cross images are published for amd64 only.
  # BUILD_VERSION reaches the container via Cross.toml; it marks the binary as a
  # local build so it is never mistaken for a published release in the logs.
  CROSS_CONTAINER_OPTS="--platform linux/amd64" \
  BUILD_VERSION="local-$(git describe --always --dirty)" \
    cross build --target "$TARGET" --release
  BINARY="target/$TARGET/release/m3i-ha-bridge"
fi

echo "=== 2. Stopping existing bridge and preparing target on $PI ==="
ssh "$PI" "sudo killall $BINARY_NAME || true && rm -f ~/$BINARY_NAME || true"

echo "=== 3. Transferring binary to $PI ==="
scp "$BINARY" "$PI:$BINARY_NAME"

echo "=== 4. Executing bridge on $PI ==="
ssh -t "$PI" "chmod +x ./$BINARY_NAME && sudo RUST_LOG=info ./$BINARY_NAME"
