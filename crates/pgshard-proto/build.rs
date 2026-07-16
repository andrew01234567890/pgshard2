use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../proto";
    // Discover entrypoints so a new .proto cannot be silently left out of the
    // Rust build while the Go side picks it up.
    let mut files: Vec<PathBuf> = std::fs::read_dir("../../proto/pgshard/v1")?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "proto"))
        .collect();
    files.sort();
    if files.is_empty() {
        return Err("no .proto files found under proto/pgshard/v1".into());
    }
    let fds = protox::compile(&files, [proto_root])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto_root}");
    Ok(())
}
