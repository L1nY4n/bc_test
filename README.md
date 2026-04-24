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
- live MQTT RX/TX log view with keyword filtering
- preset protocol command forms covering the main documented configuration/query opcodes
- raw JSON sending with opcode normalization and optional expected-response tracking
- per-device pending request tracking, timeout handling, and RTT capture
- OTA / A OTA / voice / realtime voice transfer packet generation and send queue management
- configurable transfer pacing, ACK timeout, retry budget, and retry / continue / cancel controls
- recent operation ledger and JSON evidence export
- structured payload detail decoding for core uplink/query replies
- richer per-device runtime state sinking for version, Mesh address, switch/run mode, scene, energy, A-light, group, and motion state
- dangerous-operation confirmation for multi-device sends and high-risk opcodes
- local config persistence with broker password omitted from disk
- default Chinese UI text
- automatic Chinese font loading from `assets/fonts` or common system fonts

The old Python prototype is still present in the repo as reference material, but the active path is now the Rust app.

## Run

```bash
cargo run
```

If you want to bundle your own Chinese font for more stable rendering on minimal systems, place one of these files under [assets/fonts](assets/fonts):

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

The repo now includes [release-packages.yml](.github/workflows/release-packages.yml).

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
- tag pushes such as `v0.1.1` also create or update a GitHub Release and attach the installers

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

## Remaining Gaps

1. Transfer ACK correlation is still device-scoped and timestamp-based, not packet-identity scoped.
2. Transfer retry/continue is available, but there is no durable breakpoint resume across app restarts.
3. Some transfer-related opcodes are operationally implemented through the transfer subsystem rather than exposed as standalone preset command forms.
4. The app has local verification coverage, but broker/device integration still needs real hardware validation.
