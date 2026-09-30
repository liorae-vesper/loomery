// SPDX-License-Identifier: MPL-2.0
//! Load certificate files without embedding private keys in configuration.
use crate::config::{ClientTls, ServerTls, TlsIdentity};
use anyhow::Context;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

fn certificate(bytes: Vec<u8>) -> anyhow::Result<Certificate> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&bytes) {
        roots.add(cert?)?;
    }
    anyhow::ensure!(!roots.is_empty(), "CA bundle contains no certificates");
    Ok(Certificate::from_pem(bytes))
}
async fn identity(config: &TlsIdentity) -> anyhow::Result<Identity> {
    let certificate = tokio::fs::read(&config.certificate)
        .await
        .context("read TLS certificate file")?;
    let key = tokio::fs::read(&config.private_key)
        .await
        .context("read TLS private key file")?;
    Ok(Identity::from_pem(certificate, key))
}
pub(crate) async fn client(
    config: &ClientTls,
    timeout: std::time::Duration,
) -> anyhow::Result<ClientTlsConfig> {
    let ca = tokio::fs::read(&config.ca_certificate)
        .await
        .context("read peer CA certificate file")?;
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(certificate(ca)?)
        .timeout(timeout);
    if let Some(config) = &config.identity {
        tls = tls.identity(identity(config).await?);
    }
    if let Some(name) = &config.server_name {
        tls = tls.domain_name(name);
    }
    Ok(tls)
}
pub(crate) async fn server(
    config: &ServerTls,
    timeout: std::time::Duration,
) -> anyhow::Result<ServerTlsConfig> {
    let mut tls = ServerTlsConfig::new()
        .identity(identity(&config.identity).await?)
        .timeout(timeout);
    if let Some(path) = &config.client_ca_certificate {
        let ca = tokio::fs::read(path)
            .await
            .context("read client CA certificate file")?;
        tls = tls.client_ca_root(certificate(ca)?);
    }
    Ok(tls)
}
pub(crate) async fn preflight(config: &crate::config::TransportConfig) -> anyhow::Result<()> {
    if let Some(tls) = &config.client_tls {
        tonic::transport::Endpoint::from_static("https://localhost").tls_config(
            client(
                tls,
                std::time::Duration::from_millis(config.connect_timeout_ms),
            )
            .await?,
        )?;
    }
    Ok(())
}
