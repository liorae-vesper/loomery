// SPDX-License-Identifier: MPL-2.0
//! Controlled, multi-process tonic/RocksDB consensus benchmark.
#[path = "consensus_bench/driver.rs"]
mod driver;
#[path = "consensus_bench/node.rs"]
mod node;
#[path = "consensus_bench/protocol.rs"]
mod protocol;
#[path = "consensus_bench/stats.rs"]
mod stats;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|s| s == "--node") {
        let mut spec: serde_json::Value = serde_json::from_str(
            args.get(1)
                .ok_or_else(|| anyhow::anyhow!("missing node spec"))?,
        )?;
        let config_value = spec
            .get_mut("config")
            .ok_or_else(|| anyhow::anyhow!("missing node config"))?;
        let config = protocol::Config::from_json(&serde_json::to_vec(config_value)?)?;
        *config_value = serde_json::to_value(config)?;
        let spec: protocol::NodeSpec = serde_json::from_value(spec)?;
        return runtime(spec.config.worker_threads)?.block_on(node::run(spec));
    }
    let Some((config, output)) = driver::arguments(&args)? else {
        return Ok(());
    };
    runtime(config.worker_threads)?.block_on(driver::run(config, output))
}
fn runtime(workers: usize) -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?)
}
