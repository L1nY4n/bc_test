# Mesh BC Tester (Rust)

`mesh-bc-tester-rs` is a Rust desktop MQTT test client for the Bluetooth Mesh BC light northbound protocol.

The current implementation uses:

- `eframe/egui` for the desktop GUI
- `rumqttc` for MQTT 3.1.1 client communication
- `serde_json` for JSON payload building and inspection

This direction replaced the previous PySide6 packaging path because the Rust stack is a better fit for cross-platform builds and cross-target compilation workflows.

## Current Status

The Rust app currently provides:

- broker configuration and connect/disconnect
- multi-device profile management
- per-device topic uniqueness validation
- live MQTT RX/TX log view
- preset protocol command forms
- raw JSON sending
- transfer-start preview for OTA / voice flows
- local config persistence with broker password omitted from disk
- default Chinese UI text
- automatic Chinese font loading from `assets/fonts` or common system fonts

The old Python prototype is still present in the repo as reference material, but the active path is now the Rust app.

## Run

```bash
cargo run
```

If you want to bundle your own Chinese font for more stable rendering on minimal systems, place one of these files under [assets/fonts](/Users/l1ny4n/Documents/work/bc_test/assets/fonts):

- `NotoSansSC-Regular.otf`
- `SourceHanSansCN-Regular.otf`
- `SourceHanSansSC-Regular.otf`

## Verify

```bash
cargo check
cargo test
```

## Windows amd64 Native Build

On Windows x64:

```powershell
rustup target add x86_64-pc-windows-msvc
cargo build --release --target x86_64-pc-windows-msvc
```

Output:

```text
target/x86_64-pc-windows-msvc/release/mesh-bc-tester-rs.exe
```

## Cross-Compile To Windows amd64 From macOS/Linux

Install a MinGW-w64 cross toolchain that provides `x86_64-w64-mingw32-gcc`, then run:

```bash
./scripts/build-windows-gnu-cross.sh
```

Or manually:

```bash
rustup target add x86_64-pc-windows-gnu
cargo build --release --target x86_64-pc-windows-gnu
```

Output:

```text
target/x86_64-pc-windows-gnu/release/mesh-bc-tester-rs.exe
```

## GitHub Actions Packaging

The repo now includes [release-packages.yml](/Users/l1ny4n/Documents/work/bc_test/.github/workflows/release-packages.yml).

Triggers:

- branch push: `main`, `master` (build check only)
- manual run: `workflow_dispatch`
- release tag push: `v*`

Outputs:

- Linux: `.deb`
- macOS: `.dmg`
- Windows: `.msi`

Behavior:

- branch pushes run cross-platform `cargo check`
- manual workflow runs upload the generated installers as workflow artifacts
- tag pushes such as `v0.1.0` also create or update a GitHub Release and attach the installers

Important:

- `rustup target add` only installs the Rust standard library for the target. You still need a target linker/toolchain.
- `x86_64-pc-windows-gnu` is the practical cross-target for non-Windows hosts.
- If you need MSI/installer packaging, do that on Windows after generating the `.exe`.

## Project Layout

```text
Cargo.toml
rust_src/
  main.rs
  app.rs
  models.rs
  mqtt.rs
  protocol.rs
  store.rs
scripts/
  build-windows-gnu-cross.sh
```

## Next Recommended Steps

1. Port the remaining protocol opcodes from the Python prototype into the Rust command catalog.
2. Replace transfer preview with full chunked transfer state machines.
3. Add request/response timeout tracking and richer status decoding.
4. Add CI matrix builds for macOS, Linux, and Windows.
