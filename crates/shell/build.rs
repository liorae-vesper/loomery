//! Generate the versioned Raft transport service.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/raft.proto")?;
    println!("cargo:rerun-if-changed=proto/raft.proto");
    Ok(())
}
