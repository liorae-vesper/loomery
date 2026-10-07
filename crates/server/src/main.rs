// SPDX-License-Identifier: MPL-2.0

//! `loomery-server` — the runtime entry point.
//!
//! It does three things and nothing else: read a configuration, build the real
//! adapters (NATS `JetStream` and a provider-agnostic OIDC authenticator), and
//! hand both to [`loomery_shell::host::Host`], which owns the wiring.
//!
//! ```sh
//! loomery-server --config host.json          # or LOOMERY_CONFIG=host.json
//! LOOMERY_OIDC_ISSUER=https://idp/realms/x loomery-server --config host.json
//! ```
//!
//! The host boots the control group, hosts one group per active tenant, warms
//! the identity provider, serves the gateway on `http.config.bind`, and runs the
//! outbox and saga workers until `SIGTERM`/`Ctrl-C`.
//!
//! There is no logging framework yet (tracing is Phase 7): startup, the worker
//! reports and shutdown go to stderr.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use loomery_shell::config::HostConfig;
use loomery_shell::gateway::OidcAuthenticator;
use loomery_shell::host::Host;
use loomery_shell::host::connect_nats;

/// The environment variable that carries the configuration path.
const CONFIG_VARIABLE: &str = "LOOMERY_CONFIG";

/// Runs the host; returns the process exit code.
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("loomery-server: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Loads the configuration and serves until a shutdown signal arrives.
async fn run() -> anyhow::Result<()> {
    let path = configuration_path()?;
    let config = HostConfig::load(&path)?;
    eprintln!(
        "loomery-server: node {} serving {} (data {}, control group {})",
        config.node_id,
        config.http.bind,
        config.data_dir.display(),
        config.control_group
    );

    let oidc = config
        .oidc
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no identity provider is configured: set `oidc` in {} or LOOMERY_OIDC_ISSUER \
                 (the gateway fails closed without one)",
                path.display()
            )
        })?
        .clone();
    let authenticator = Arc::new(OidcAuthenticator::new(oidc)?);
    authenticator
        .warm()
        .await
        .map_err(|_| anyhow::anyhow!("the identity provider could not be reached for its keys"))?;
    eprintln!(
        "loomery-server: identity provider {} is reachable",
        authenticator.issuer()
    );

    let (publisher, consumer) = if let Some(nats) = &config.nats {
        let (publisher, consumer) = connect_nats(nats).await?;
        eprintln!("loomery-server: broker {} is reachable", nats.url);
        (Some(publisher), Some(consumer))
    } else {
        eprintln!("loomery-server: no broker configured; the outbox and sagas are off");
        (None, None)
    };

    let mut host = Host::boot(config, authenticator, publisher.clone(), consumer.clone()).await?;
    host.start_workers().await?;

    for (organization_id, tenant) in host.incomplete_tenants().await {
        eprintln!(
            "loomery-server: {organization_id} is still provisioning ({:?}); \
             resume it with the original bootstrap",
            tenant.status
        );
    }
    eprintln!(
        "loomery-server: hosting {} group(s): {:?}",
        host.groups().len(),
        host.groups().ids()
    );

    tokio::select! {
        served = host.serve() => served?,
        () = shutdown_signal() => eprintln!("loomery-server: shutting down"),
    }

    host.shutdown().await?;
    Ok(())
}

/// The configuration path: `--config <path>` or `LOOMERY_CONFIG`.
fn configuration_path() -> anyhow::Result<PathBuf> {
    let mut arguments = std::env::args().skip(1);
    if let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                let path = arguments
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--config needs a path"))?;
                return Ok(PathBuf::from(path));
            }
            "--help" | "-h" => {
                println!(
                    "loomery-server [--config <path>]\n\n\
                     Configuration is JSON; `LOOMERY_CONFIG` names the file and the \
                     `LOOMERY_*` environment variables override it."
                );
                std::process::exit(0);
            }
            other => anyhow::bail!(
                "unknown argument {other:?}: expected --config <path> (or LOOMERY_CONFIG)"
            ),
        }
    }

    std::env::var(CONFIG_VARIABLE)
        .map(PathBuf::from)
        .map_err(|_| {
            anyhow::anyhow!("no configuration: pass --config <path> or set {CONFIG_VARIABLE}")
        })
}

/// Resolves when the process is asked to stop.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                eprintln!("loomery-server: cannot listen for SIGTERM: {error}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
