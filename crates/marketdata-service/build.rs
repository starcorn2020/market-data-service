//! Build-time hook: compile `proto/marketdata.proto` into Rust via `tonic-build`.
//!
//! Output lands in `$OUT_DIR/marketdata.v1.rs`; `src/grpc.rs` pulls it in via
//! `tonic::include_proto!("marketdata.v1")`.
//!
//! Reviewer ergonomics: we route through `protoc-bin-vendored` so that **no
//! system `protoc` is required**. The build-time env var `PROTOC` is what
//! `tonic-build` looks at; setting it to the vendored binary makes the build
//! reproducible across macOS / Linux / Windows without extra setup.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored protoc -> set PROTOC for tonic-build / prost-build to pick up.
    let protoc: PathBuf = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build.rs runs single-threaded; setting an env var during build
    // configuration is the documented way to direct tonic-build to a custom
    // protoc. No concurrent reads from this process.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
    }

    // Relative path from this crate's manifest dir (= crates/marketdata-service/)
    // up to the workspace-root proto file.
    let proto = "../../proto/marketdata.proto";

    tonic_build::configure()
        // We only need server stubs in the service crate proper; the sample
        // client (src/bin/client.rs) also lives in this crate, so it'll reuse
        // the same generated module via `tonic::include_proto!`. Hence both
        // server and client are kept enabled.
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["../../proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
