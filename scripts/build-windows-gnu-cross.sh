#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-x86_64-pc-windows-gnu}"
MODE="${MODE:-release}"

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup is required" >&2
  exit 1
fi

if ! command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
  echo "x86_64-w64-mingw32-gcc not found. Install a mingw-w64 cross toolchain first." >&2
  exit 1
fi

rustup target add "$TARGET"

if [ "$MODE" = "release" ]; then
  cargo build --release --target "$TARGET"
else
  cargo build --target "$TARGET"
fi

echo "Windows GNU build finished:"
echo "target/$TARGET/$MODE/"
