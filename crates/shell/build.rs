//! Generate the versioned Raft transport service, and link the C++ runtime of a
//! prebuilt `RocksDB` when the build was handed one.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/raft.proto")?;
    println!("cargo:rerun-if-changed=proto/raft.proto");
    link_cxx_runtime_of_a_prebuilt_rocksdb();
    Ok(())
}

/// Asks the linker for the C++ runtime when `librocksdb-sys` did not build
/// `RocksDB` itself.
///
/// `librocksdb-sys` compiles `RocksDB`'s C++ sources in its build script, and that
/// script is the only thing that emits `cargo:rustc-link-lib=stdc++` — `cc`'s
/// `cpp_link_stdlib`, at the end of `build_rocksdb()`. Setting `ROCKSDB_LIB_DIR`
/// makes the script take `try_to_find_and_link_lib` instead: it emits
/// `rustc-link-search` and `rustc-link-lib` for the archive, returns before
/// `build_rocksdb()`, and the standard library's directive goes with it. The
/// archive then links with every `std::` symbol undefined.
///
/// An environment that hands the build a prebuilt archive (by setting
/// `ROCKSDB_LIB_DIR`) is the case this covers, and the directive has to come
/// from here instead. GitHub Actions does not prebuild one, so the vendored
/// build is used there and this emits nothing. It is deliberately conditional:
/// with the vendored build, the directive is emitted twice for the same link.
fn link_cxx_runtime_of_a_prebuilt_rocksdb() {
    println!("cargo:rerun-if-env-changed=ROCKSDB_LIB_DIR");
    if std::env::var_os("ROCKSDB_LIB_DIR").is_none() {
        return;
    }

    // The names match the ones `librocksdb-sys` picks for the same targets.
    let stdlib = match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("linux") => Some("stdc++"),
        Ok("windows") => None,
        _ => Some("c++"),
    };
    if let Some(stdlib) = stdlib {
        println!("cargo:rustc-link-lib={stdlib}");
    }
}
