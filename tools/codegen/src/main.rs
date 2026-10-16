//! Regenerates `src/proto/*.rs` from `proto/envd`. Needs `protoc` on PATH.
//! Run from the repo root: `cargo run --manifest-path tools/codegen/Cargo.toml`.
fn main() {
    let mut cfg = prost_build::Config::new();
    cfg.out_dir("src/proto");
    cfg.bytes(["."]);
    cfg.compile_protos(
        &[
            "proto/envd/process/process.proto",
            "proto/envd/filesystem/filesystem.proto",
        ],
        &["proto/envd"],
    )
    .expect("protoc failed");
}
