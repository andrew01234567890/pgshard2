use std::path::{Path, PathBuf};

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_protos(&path, out)?;
        } else if file_type.is_file() && path.extension().is_some_and(|e| e == "proto") {
            out.push(path);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../proto";
    // Recursively discover entrypoints, mirroring buf's module glob, so a new
    // .proto anywhere under proto/ cannot be silently left out of the Rust
    // build while the Go side picks it up.
    let mut files = Vec::new();
    collect_protos(Path::new(proto_root), &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err("no .proto files found under proto/".into());
    }
    let fds = protox::compile(&files, [proto_root])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto_root}");
    Ok(())
}
