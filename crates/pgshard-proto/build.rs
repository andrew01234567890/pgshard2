fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../proto";
    let files = [
        "../../proto/pgshard/v1/common.proto",
        "../../proto/pgshard/v1/topology.proto",
        "../../proto/pgshard/v1/workflow.proto",
        "../../proto/pgshard/v1/vstream.proto",
        "../../proto/pgshard/v1/agent.proto",
        "../../proto/pgshard/v1/router_admin.proto",
    ];
    let fds = protox::compile(files, [proto_root])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed={proto_root}");
    Ok(())
}
