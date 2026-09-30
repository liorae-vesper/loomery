// SPDX-License-Identifier: MPL-2.0
//! Real TLS handshakes: trusted peers, hostname checks, and mandatory mTLS.
use super::{
    RaftGroup,
    transport::{TonicNetworkFactory, TonicTransport},
};
use crate::config::{ClientTls, GroupConfig, ServerTls, TlsIdentity, TransportConfig};
use openraft::{
    BasicNode, Vote,
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::VoteRequest,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::{path::Path, time::Duration};

pub(super) fn certificates(root: &Path) -> (ServerTls, ClientTls) {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &issuer).unwrap();
    let ca_path = root.join("ca.pem");
    let identity = TlsIdentity {
        certificate: root.join("cert.pem"),
        private_key: root.join("key.pem"),
    };
    std::fs::write(&ca_path, ca.pem()).unwrap();
    std::fs::write(&identity.certificate, cert.pem()).unwrap();
    std::fs::write(&identity.private_key, key.serialize_pem()).unwrap();
    (
        ServerTls {
            identity: identity.clone(),
            client_ca_certificate: None,
        },
        ClientTls {
            ca_certificate: ca_path,
            identity: Some(identity),
            server_name: Some("localhost".into()),
        },
    )
}
async fn vote(address: &str, tls: Option<ClientTls>) -> bool {
    let mut factory = TonicNetworkFactory {
        group_id: "tls".into(),
        config: TransportConfig {
            client_tls: tls,
            ..TransportConfig::default()
        },
    };
    let mut client = factory.new_client(1, &BasicNode::new(address)).await;
    client
        .vote(
            VoteRequest::new(Vote::new(10, 2), None),
            RPCOption::new(Duration::from_secs(3)),
        )
        .await
        .is_ok()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_and_mutual_tls_enforce_trust_and_identity() {
    for mutual in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (mut server_tls, client_tls) = certificates(root.path());
        if mutual {
            server_tls.client_ca_certificate = Some(client_tls.ca_certificate.clone());
        }
        let config = TransportConfig {
            server_tls: Some(server_tls),
            ..TransportConfig::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = listener.local_addr().unwrap();
        let address = format!("https://{socket}");
        let group = RaftGroup::boot_persistent(
            1,
            "tls".into(),
            &root.path().join("db"),
            GroupConfig::default(),
        )
        .await
        .unwrap();
        let transport = TonicTransport::default();
        transport
            .register("tls".into(), group.raft())
            .await
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            transport
                .serve(listener, config, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        assert!(vote(&address, Some(client_tls.clone())).await);
        let mut anonymous = client_tls.clone();
        anonymous.identity = None;
        assert_eq!(vote(&address, Some(anonymous)).await, !mutual);
        let mut wrong_name = client_tls.clone();
        wrong_name.server_name = Some("wrong.example".into());
        assert!(!vote(&address, Some(wrong_name)).await);
        let other = tempfile::tempdir().unwrap();
        let (_, mut wrong_ca) = certificates(other.path());
        wrong_ca.identity = client_tls.identity.clone();
        assert!(!vote(&address, Some(wrong_ca)).await);
        assert!(!vote(&format!("http://{socket}"), Some(client_tls)).await);
        assert!(!vote(&address, None).await);
        assert!(!vote(&format!("http://{socket}"), None).await);
        group.shutdown().await.unwrap();
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}
#[tokio::test]
async fn invalid_tls_configuration_fails_before_storage_startup() {
    let root = tempfile::tempdir().unwrap();
    let (mut server_tls, mut client_tls) = certificates(root.path());
    client_tls.ca_certificate = root.path().join("missing.pem");
    let mut config = GroupConfig::default();
    config.transport.client_tls = Some(client_tls);
    let path = root.path().join("db");
    assert!(
        RaftGroup::boot_persistent(1, "tls".into(), &path, config.clone())
            .await
            .is_err()
    );
    assert!(!path.exists());
    config.transport.client_tls.as_mut().unwrap().ca_certificate = root.path().join("bad.pem");
    std::fs::write(root.path().join("bad.pem"), "not a certificate").unwrap();
    assert!(
        RaftGroup::boot_persistent(1, "tls".into(), &path, config.clone())
            .await
            .is_err()
    );
    config.transport.client_tls.as_mut().unwrap().server_name = Some(String::new());
    assert!(config.validate().is_err());
    let serialized = serde_json::to_string(&config).unwrap();
    assert!(!serialized.contains("PRIVATE KEY"));
    server_tls.identity.private_key = root.path().join("bad.pem");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = TransportConfig {
        server_tls: Some(server_tls),
        ..TransportConfig::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        TonicTransport::default().serve(listener, config, std::future::pending()),
    )
    .await
    .unwrap();
    assert!(result.is_err());
}
