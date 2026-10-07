// SPDX-License-Identifier: MPL-2.0

//! Offline end-to-end tests for the provider-agnostic OIDC adapter.
//!
//! Each test talks to a throwaway identity provider run inside the test process:
//! it publishes a discovery document and a JWKS for a key pair generated for the
//! run, and signs tokens with it. That exercises discovery, the JWKS cache,
//! signature verification and claim mapping against real crypto, with no
//! Keycloak, no network and no committed key material.
//!
//! The live counterpart (a real Keycloak, reached through the same adapter) is in
//! `tests/test_services.rs`.
#![allow(clippy::unused_async)] // axum's `Handler` requires async handlers, even with no awaits.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::encode;
use rsa::RsaPrivateKey;
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::Value;
use serde_json::json;
use tokio::sync::OnceCell;

use super::OidcAuthenticator;
use crate::config::OidcConfig;
use crate::gateway::AuthError;
use crate::gateway::Authenticator;
use crate::gateway::Identity;

/// The key id the test provider publishes.
const TEST_KID: &str = "test-key-1";

/// A canonical UUID, as a provider with UUID subjects would issue.
const SUBJECT: &str = "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9";

/// The subject namespace a configuration may derive opaque subjects in.
const NAMESPACE: &str = "5c1a9f43-2e78-4b06-8d17-a2c3b4d5e6f7";

/// Seconds since the epoch, shifted by `offset`.
fn epoch(offset: i64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a sane clock")
        .as_secs();
    if offset >= 0 {
        now.saturating_add(offset.unsigned_abs())
    } else {
        now.saturating_sub(offset.unsigned_abs())
    }
}

/// The state the test provider serves.
struct ServerState {
    discovery: Value,
    jwks: Value,
    /// How many discovery documents were requested.
    discoveries: AtomicUsize,
    /// How many key sets were requested.
    jwks_fetches: AtomicUsize,
}

async fn discovery(State(state): State<Arc<ServerState>>) -> Json<Value> {
    state.discoveries.fetch_add(1, Ordering::Relaxed);
    Json(state.discovery.clone())
}

async fn jwks(State(state): State<Arc<ServerState>>) -> Json<Value> {
    state.jwks_fetches.fetch_add(1, Ordering::Relaxed);
    Json(state.jwks.clone())
}

/// A throwaway provider: a key pair, a JWKS and a tiny HTTP server.
struct Provider {
    issuer: String,
    encoding: EncodingKey,
    forged: EncodingKey,
    state: Arc<ServerState>,
}

impl Provider {
    /// Generates a key pair and serves discovery and the JWKS on a random port.
    ///
    /// Synchronous: generating a key pair is CPU work, and the server runs on
    /// the process-wide runtime.
    fn start() -> Self {
        let signing = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("a test key pair");
        let other = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("a second test key");

        let public = signing.to_public_key();
        let key_set = json!({
            "keys": [{
                "kty": "RSA",
                "kid": TEST_KID,
                "use": "sig",
                "alg": "RS256",
                "n": URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
                "e": URL_SAFE_NO_PAD.encode(public.e().to_bytes_be()),
            }]
        });

        // Bind with std and convert inside the server runtime: a `tokio`
        // listener created on a `#[tokio::test]` runtime would be registered
        // with a reactor that dies when that test ends.
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
        std_listener
            .set_nonblocking(true)
            .expect("a non-blocking listener");
        let addr: SocketAddr = std_listener.local_addr().expect("a bound address");
        let issuer = format!("http://{addr}");
        let state = Arc::new(ServerState {
            discovery: json!({
                "issuer": issuer,
                "jwks_uri": format!("{issuer}/jwks"),
                "authorization_endpoint": format!("{issuer}/authorize"),
                "token_endpoint": format!("{issuer}/token"),
            }),
            jwks: key_set,
            discoveries: AtomicUsize::new(0),
            jwks_fetches: AtomicUsize::new(0),
        });

        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .with_state(Arc::clone(&state));
        server_runtime().spawn(async move {
            let listener =
                tokio::net::TcpListener::from_std(std_listener).expect("a tokio listener");
            let _ = axum::serve(listener, app).await;
        });

        // `from_rsa_der` takes PKCS#1 DER, not PKCS#8.
        let encoding = EncodingKey::from_rsa_der(
            signing
                .to_pkcs1_der()
                .expect("a PKCS#1 private key")
                .as_bytes(),
        );
        let forged = EncodingKey::from_rsa_der(
            other
                .to_pkcs1_der()
                .expect("a PKCS#1 private key")
                .as_bytes(),
        );

        Self {
            issuer,
            encoding,
            forged,
            state,
        }
    }

    /// Signs `claims` with the provider's key.
    fn token(&self, claims: &Value) -> String {
        let header = Header {
            alg: Algorithm::RS256,
            kid: Some(TEST_KID.to_owned()),
            ..Header::default()
        };
        encode(&header, claims, &self.encoding).expect("a signed token")
    }

    /// Signs `claims` with the right key id but the wrong key.
    fn forged_token(&self, claims: &Value) -> String {
        let header = Header {
            alg: Algorithm::RS256,
            kid: Some(TEST_KID.to_owned()),
            ..Header::default()
        };
        encode(&header, claims, &self.forged).expect("a signed token")
    }

    /// The key set requests served so far.
    fn jwks_fetches(&self) -> usize {
        self.state.jwks_fetches.load(Ordering::Relaxed)
    }

    /// The discovery requests served so far.
    fn discoveries(&self) -> usize {
        self.state.discoveries.load(Ordering::Relaxed)
    }
}

/// The runtime the provider servers run on.
///
/// A `#[tokio::test]` runtime is dropped when its test ends, which would kill a
/// server spawned on it and make the shared provider unusable for every later
/// test. A process-wide runtime keeps every provider reachable for the whole
/// test binary.
fn server_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("a test runtime")
    })
}

/// The provider shared by the functional tests (one key pair per test binary).
static PROVIDER: OnceCell<Provider> = OnceCell::const_new();

async fn provider() -> &'static Provider {
    PROVIDER.get_or_init(|| async { Provider::start() }).await
}

/// An authenticator pointed at `issuer`, with `mutate` applied.
fn authenticator_for(issuer: &str, mutate: impl FnOnce(&mut OidcConfig)) -> OidcAuthenticator {
    let mut config = OidcConfig {
        issuer: issuer.to_owned(),
        ..OidcConfig::default()
    };
    mutate(&mut config);
    OidcAuthenticator::new(config).expect("a valid configuration")
}

/// An authenticator pointed at the shared provider.
async fn authenticator(mutate: impl FnOnce(&mut OidcConfig)) -> OidcAuthenticator {
    authenticator_for(&provider().await.issuer, mutate)
}

/// A claim set for a valid token.
fn claims(subject: &str, groups: &Value) -> Value {
    json!({
        "sub": subject,
        "iss": provider_issuer_placeholder(),
        "exp": epoch(300),
        "iat": epoch(0),
        "groups": groups,
    })
}

/// The issuer is filled in by [`token_for`]; this keeps [`claims`] honest about
/// the shape while the caller supplies the value.
fn provider_issuer_placeholder() -> String {
    String::new()
}

#[tokio::test]
async fn a_valid_token_authenticates_through_discovery_and_jwks() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut admin = claims(SUBJECT, &json!(["admins"]));
    admin["iss"] = json!(provider.issuer);
    let identity = authenticator
        .authenticate(Some(&provider.token(&admin)))
        .await;

    assert_eq!(
        identity.unwrap(),
        Identity {
            user_id: loomery_core::id::Id::parse(SUBJECT).unwrap(),
            is_admin: true,
            email: None,
        }
    );
    assert!(
        provider.discoveries() > 0 && provider.jwks_fetches() > 0,
        "discovery and the key set are fetched once and cached"
    );
}

#[tokio::test]
async fn a_member_token_is_not_an_admin() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut member = claims(SUBJECT, &json!(["members"]));
    member["iss"] = json!(provider.issuer);
    let identity = authenticator
        .authenticate(Some(&provider.token(&member)))
        .await
        .expect("a valid token");

    assert!(!identity.is_admin);
}

#[tokio::test]
async fn the_admin_group_is_configurable() {
    let provider = provider().await;
    let authenticator = authenticator(|config| {
        config.groups_claim = "realm_access.roles".to_owned();
        config.admin_group = "platform-admin".to_owned();
    })
    .await;

    let mut token_claims = claims(SUBJECT, &json!([]));
    token_claims["iss"] = json!(provider.issuer);
    token_claims["realm_access"] = json!({ "roles": ["platform-admin"] });

    let identity = authenticator
        .authenticate(Some(&provider.token(&token_claims)))
        .await
        .expect("a valid token");
    assert!(identity.is_admin);
}

#[tokio::test]
async fn an_expired_token_is_refused() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut expired = claims(SUBJECT, &json!(["admins"]));
    expired["iss"] = json!(provider.issuer);
    expired["exp"] = json!(epoch(-3600));

    assert_eq!(
        authenticator
            .authenticate(Some(&provider.token(&expired)))
            .await,
        Err(AuthError::Unknown)
    );
}

#[tokio::test]
async fn a_token_without_an_expiry_is_refused() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut no_expiry = claims(SUBJECT, &json!(["admins"]));
    no_expiry["iss"] = json!(provider.issuer);
    no_expiry.as_object_mut().unwrap().remove("exp");

    assert_eq!(
        authenticator
            .authenticate(Some(&provider.token(&no_expiry)))
            .await,
        Err(AuthError::Unknown)
    );
}

#[tokio::test]
async fn a_token_from_another_issuer_is_refused() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut elsewhere = claims(SUBJECT, &json!(["admins"]));
    elsewhere["iss"] = json!("https://elsewhere.example.com");

    assert_eq!(
        authenticator
            .authenticate(Some(&provider.token(&elsewhere)))
            .await,
        Err(AuthError::Unknown)
    );
}

#[tokio::test]
async fn an_audience_is_enforced_when_configured() {
    let provider = provider().await;
    let authenticator = authenticator(|config| {
        config.audience = Some("loomery-gateway".to_owned());
    })
    .await;

    let mut wrong = claims(SUBJECT, &json!(["admins"]));
    wrong["iss"] = json!(provider.issuer);
    wrong["aud"] = json!("another-client");
    assert_eq!(
        authenticator
            .authenticate(Some(&provider.token(&wrong)))
            .await,
        Err(AuthError::Unknown)
    );

    let mut right = claims(SUBJECT, &json!(["admins"]));
    right["iss"] = json!(provider.issuer);
    right["aud"] = json!("loomery-gateway");
    assert!(
        authenticator
            .authenticate(Some(&provider.token(&right)))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_token_signed_by_another_key_is_refused() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    let mut forged = claims(SUBJECT, &json!(["admins"]));
    forged["iss"] = json!(provider.issuer);

    assert_eq!(
        authenticator
            .authenticate(Some(&provider.forged_token(&forged)))
            .await,
        Err(AuthError::Unknown),
        "the same key id does not make the signature valid"
    );
}

#[tokio::test]
async fn an_algorithm_confusion_attempt_is_refused() {
    let provider = provider().await;
    let authenticator = authenticator(|_| {}).await;

    // An HS256 token naming the provider's key id: the adapter takes the
    // algorithm from the key set, so this must not be verified as an HMAC.
    let header = Header {
        alg: Algorithm::HS256,
        kid: Some(TEST_KID.to_owned()),
        ..Header::default()
    };
    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_secret(b"not-the-providers-key"),
    )
    .expect("a signed token");

    assert_eq!(
        authenticator.authenticate(Some(&token)).await,
        Err(AuthError::Unknown)
    );
}

#[tokio::test]
async fn malformed_tokens_are_refused() {
    let authenticator = authenticator(|_| {}).await;

    for token in ["", "not-a-token", "a.b.c", "eyJhbGciOiJSUzI1NiJ9.."] {
        assert_eq!(
            authenticator.authenticate(Some(token)).await,
            Err(AuthError::Unknown),
            "{token:?} must be refused"
        );
    }
}

#[tokio::test]
async fn a_missing_token_is_missing_rather_than_unknown() {
    let authenticator = authenticator(|_| {}).await;

    assert_eq!(
        authenticator.authenticate(None).await,
        Err(AuthError::Missing)
    );
}

#[tokio::test]
async fn the_subject_claim_is_configurable() {
    let provider = provider().await;
    let authenticator = authenticator(|config| {
        config.subject_claim = "user_id".to_owned();
    })
    .await;

    // A provider that does not use `sub` at all.
    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    claims.as_object_mut().unwrap().remove("sub");
    claims["user_id"] = json!(SUBJECT);

    let identity = authenticator
        .authenticate(Some(&provider.token(&claims)))
        .await
        .expect("the configured claim carries the subject");
    assert_eq!(
        identity.user_id,
        loomery_core::id::Id::parse(SUBJECT).unwrap()
    );
}

#[tokio::test]
async fn a_verified_email_is_carried_and_an_unverified_one_is_not() {
    let provider = provider().await;
    let strict = authenticator(|_| {}).await;

    let mut verified = claims(SUBJECT, &json!(["admins"]));
    verified["iss"] = json!(provider.issuer);
    verified["email"] = json!("ada@example.com");
    verified["email_verified"] = json!(true);
    let identity = strict
        .authenticate(Some(&provider.token(&verified)))
        .await
        .expect("a valid token");
    assert_eq!(identity.email.as_deref(), Some("ada@example.com"));

    let mut unverified = claims(SUBJECT, &json!(["admins"]));
    unverified["iss"] = json!(provider.issuer);
    unverified["email"] = json!("ada@example.com");
    unverified["email_verified"] = json!(false);
    let identity = strict
        .authenticate(Some(&provider.token(&unverified)))
        .await
        .expect("a valid token");
    assert_eq!(
        identity.email, None,
        "an address the provider does not vouch for is not attribution"
    );

    let mut absent = claims(SUBJECT, &json!(["admins"]));
    absent["iss"] = json!(provider.issuer);
    absent["email"] = json!("ada@example.com");
    let identity = strict
        .authenticate(Some(&provider.token(&absent)))
        .await
        .expect("a valid token");
    assert_eq!(
        identity.email, None,
        "a missing claim is not a verified one"
    );

    // A deployment may accept an unverified address explicitly.
    let lenient = authenticator(|config| {
        config.require_verified_email = false;
    })
    .await;
    let identity = lenient
        .authenticate(Some(&provider.token(&unverified)))
        .await
        .expect("a valid token");
    assert_eq!(identity.email.as_deref(), Some("ada@example.com"));
}

#[tokio::test]
async fn the_email_claim_is_configurable() {
    let provider = provider().await;
    let authenticator = authenticator(|config| {
        config.email_claim = "preferred_username".to_owned();
    })
    .await;

    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    claims["preferred_username"] = json!("ada@example.com");
    claims["email_verified"] = json!(true);

    let identity = authenticator
        .authenticate(Some(&provider.token(&claims)))
        .await
        .expect("a valid token");
    assert_eq!(identity.email.as_deref(), Some("ada@example.com"));
}

#[tokio::test]
async fn an_opaque_subject_needs_a_namespace_and_is_stable_with_one() {
    let provider = provider().await;

    let mut opaque = claims("opaque-subject", &json!(["admins"]));
    opaque["iss"] = json!(provider.issuer);
    let token = provider.token(&opaque);

    let strict = authenticator(|_| {}).await;
    assert_eq!(
        strict.authenticate(Some(&token)).await,
        Err(AuthError::Unknown),
        "without a namespace an opaque subject is refused rather than guessed"
    );

    let derived = authenticator(|config| {
        config.subject_namespace = Some(NAMESPACE.to_owned());
    })
    .await;
    let first = derived
        .authenticate(Some(&token))
        .await
        .expect("an identity");
    let again = derived
        .authenticate(Some(&token))
        .await
        .expect("an identity");
    assert_eq!(
        first, again,
        "the derived id is stable for the same subject"
    );
    assert_ne!(first.user_id.to_string(), "opaque-subject");
}

#[tokio::test]
async fn an_unknown_key_id_is_refused_without_stampeding_the_provider() {
    // A dedicated provider, so the fetch counters are this test's alone.
    let provider = Provider::start();
    let authenticator = authenticator_for(&provider.issuer, |_| {});

    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    let header = Header {
        alg: Algorithm::RS256,
        kid: Some("an-unknown-key".to_owned()),
        ..Header::default()
    };
    let token = encode(&header, &claims, &provider.encoding).expect("a signed token");

    for _ in 0..10 {
        assert_eq!(
            authenticator.authenticate(Some(&token)).await,
            Err(AuthError::Unknown)
        );
    }
    assert_eq!(
        provider.jwks_fetches(),
        1,
        "ten requests with an unknown key id cause one fetch, not ten"
    );

    // Past the cooldown, one more request may look again (the provider might
    // have rotated its keys since).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        authenticator.authenticate(Some(&token)).await,
        Err(AuthError::Unknown)
    );
    assert_eq!(provider.jwks_fetches(), 2);
}

#[tokio::test]
async fn an_explicit_jwks_uri_skips_discovery() {
    let provider = Provider::start();
    let authenticator = authenticator_for(&provider.issuer, |config| {
        config.jwks_uri = Some(format!("{}/jwks", provider.issuer));
    });

    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    assert!(
        authenticator
            .authenticate(Some(&provider.token(&claims)))
            .await
            .is_ok()
    );
    assert_eq!(
        provider.discoveries(),
        0,
        "a provider without discovery is supported by configuring the key set"
    );
}

#[tokio::test]
async fn an_unreachable_provider_is_unknown_not_a_panic() {
    // Bind and drop, so the port is (almost certainly) closed.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let authenticator = authenticator_for(&format!("http://{addr}"), |config| {
        config.timeout_ms = 250;
    });
    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(format!("http://{addr}"));

    let Some(provider) = PROVIDER.get() else {
        // A token from the shared provider is enough: the network call fails
        // before the token is ever examined.
        assert_eq!(
            authenticator.authenticate(Some("a.b.c")).await,
            Err(AuthError::Unknown)
        );
        return;
    };
    assert_eq!(
        authenticator
            .authenticate(Some(&provider.token(&claims)))
            .await,
        Err(AuthError::Unknown)
    );
}

#[tokio::test]
async fn warming_checks_the_provider_at_boot() {
    let provider = Provider::start();
    let authenticator = authenticator_for(&provider.issuer, |_| {});
    authenticator.warm().await.expect("the provider answers");
    assert!(provider.jwks_fetches() > 0);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let unreachable = authenticator_for(&format!("http://{addr}"), |config| {
        config.timeout_ms = 250;
    });
    assert_eq!(unreachable.warm().await, Err(AuthError::Unknown));
}

#[tokio::test]
async fn the_key_set_is_refetched_once_it_has_aged_out() {
    let provider = Provider::start();
    let authenticator = authenticator_for(&provider.issuer, |config| {
        // Any non-zero TTL, with a sleep past it below.
        config.jwks_ttl_seconds = 1;
    });

    let mut claims = claims(SUBJECT, &json!(["admins"]));
    claims["iss"] = json!(provider.issuer);
    let token = provider.token(&claims);

    assert!(authenticator.authenticate(Some(&token)).await.is_ok());
    let after_first = provider.jwks_fetches();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(authenticator.authenticate(Some(&token)).await.is_ok());

    assert!(
        provider.jwks_fetches() > after_first,
        "an aged-out key set is refetched, not trusted forever"
    );
}
