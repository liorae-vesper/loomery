// SPDX-License-Identifier: MPL-2.0

//! Provider-agnostic OIDC: discovery, JWKS and local JWT validation.
//!
//! This is the runtime identity adapter. Nothing here is Keycloak-specific:
//!
//! * the only required setting is the **issuer** — the key set comes from the
//!   provider's own `/.well-known/openid-configuration` (or an explicit
//!   [`OidcConfig::jwks_uri`] for providers without discovery);
//! * tokens are validated **locally**: signature against the JWKS entry whose
//!   `kid` the token names, `exp`/`nbf` with a configured leeway, `iss`, and
//!   `aud` when the provider sets one. No per-request round trip to the provider;
//! * the **claim names are configuration**: [`OidcConfig::subject_claim`] names
//!   the user id (`sub` by default), [`OidcConfig::email_claim`] the address,
//!   [`OidcConfig::groups_claim`] is a dot path (`groups`, `realm_access.roles`,
//!   `https://example.com/roles`, …) and [`OidcConfig::admin_group`] names the
//!   value that grants `is_admin`;
//! * the **subject** becomes an [`Id`] either directly (a canonical UUID, which
//!   Keycloak and most providers use) or through a configured namespace
//!   (`UUIDv5`), so a provider with opaque subjects still yields a stable id (D12).
//!
//! Failures are fail-closed and indistinguishable to the caller
//! ([`AuthError::Unknown`]), so a token cannot probe which check rejected it.
//!
//! Known limits, deliberately: only RSA, EC (P-256/P-384) and `EdDSA` keys are
//! accepted (OKP keys are skipped: the JWT library exposes no component
//! constructor for them); a JWK without a `kid` cannot be selected; and the
//! algorithm is taken from the **key**, never from the token header.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use jsonwebtoken::decode;
use jsonwebtoken::decode_header;
use loomery_core::Uuid;
use loomery_core::id::Id;
use loomery_core::key::Key;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use super::AuthError;
use super::AuthFuture;
use super::Authenticator;
use super::Identity;
use crate::config::OidcConfig;

/// How long a failed key refresh is remembered before another is attempted.
///
/// Without this, a token naming an unknown `kid` would make the gateway fetch the
/// key set on every request — a cheap way to turn the `IdP` into the bottleneck.
const REFRESH_COOLDOWN: Duration = Duration::from_secs(1);

/// The part of the discovery document this adapter needs.
#[derive(Debug, Deserialize)]
struct Discovery {
    /// Where the provider publishes its signing keys.
    jwks_uri: String,
}

/// One JSON Web Key.
#[derive(Debug, Deserialize)]
struct Jwk {
    /// The key id a token's header names.
    kid: Option<String>,
    /// The key type (`RSA`, `EC`, `OKP`).
    kty: String,
    /// The algorithm the key is meant for, when published.
    #[serde(default)]
    alg: Option<String>,
    /// The curve, for `EC`.
    #[serde(default)]
    crv: Option<String>,
    /// RSA modulus / EC x coordinate / `EdDSA` public key (base64url).
    #[serde(default)]
    n: Option<String>,
    /// RSA exponent (base64url).
    #[serde(default)]
    e: Option<String>,
    /// EC x coordinate (base64url).
    #[serde(default)]
    x: Option<String>,
    /// EC y coordinate (base64url).
    #[serde(default)]
    y: Option<String>,
}

/// A published key set.
#[derive(Debug, Deserialize)]
struct Jwks {
    /// The keys.
    keys: Vec<Jwk>,
}

/// A signing key and the algorithm it may be used with.
type SigningKey = (DecodingKey, Algorithm);

/// The cached key set and the state of the last fetch.
#[derive(Default)]
struct Keys {
    /// The discovered JWKS URL, once known.
    jwks_uri: Option<String>,
    /// The signing keys, by `kid`.
    entries: HashMap<String, SigningKey>,
    /// When the current set was fetched.
    fetched_at: Option<Instant>,
    /// When a fetch was last *attempted*, successful or not.
    attempted_at: Option<Instant>,
}

impl Keys {
    /// Whether `kid` is available and the set is still trusted.
    fn fresh(&self, kid: &str, ttl_seconds: u64) -> Option<SigningKey> {
        let fetched_at = self.fetched_at?;
        if fetched_at.elapsed() >= Duration::from_secs(ttl_seconds) {
            return None;
        }
        self.entries.get(kid).cloned()
    }

    /// Whether a refresh is allowed right now.
    fn may_refresh(&self) -> bool {
        self.attempted_at
            .is_none_or(|attempted| attempted.elapsed() >= REFRESH_COOLDOWN)
    }
}

/// Authenticates bearer tokens against any OIDC provider.
pub struct OidcAuthenticator {
    config: OidcConfig,
    issuer: String,
    namespace: Option<Uuid>,
    http: reqwest::Client,
    keys: Mutex<Keys>,
}

impl OidcAuthenticator {
    /// Builds an authenticator from `config`.
    ///
    /// No network call happens here: the key set is fetched on the first
    /// authentication, or eagerly with [`OidcAuthenticator::warm`].
    ///
    /// # Errors
    ///
    /// The configuration is invalid (see
    /// [`OidcConfig::validate`](crate::config::OidcConfig::validate)) or the
    /// subject namespace is not a UUID.
    pub fn new(config: OidcConfig) -> anyhow::Result<Self> {
        config.validate()?;
        install_tls_provider();

        let namespace = match &config.subject_namespace {
            Some(namespace) => Some(
                Uuid::parse_str(namespace)
                    .map_err(|_| anyhow::anyhow!("oidc.subject_namespace must be a UUID"))?,
            ),
            None => None,
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()?;

        Ok(Self {
            issuer: config.issuer.trim_end_matches('/').to_owned(),
            config,
            namespace,
            http,
            keys: Mutex::new(Keys::default()),
        })
    }

    /// Fetches the key set now.
    ///
    /// A host calls this at boot so a misconfigured provider fails at startup
    /// rather than on the first request.
    ///
    /// # Errors
    ///
    /// [`AuthError::Unknown`] when discovery or the key set cannot be read.
    pub async fn warm(&self) -> Result<(), AuthError> {
        let mut keys = self.keys.lock().await;
        self.refresh(&mut keys).await
    }

    /// The configured issuer, without a trailing slash.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The signing key named by `kid`, refreshing once when it is unknown or the
    /// cached set has aged out.
    async fn key(&self, kid: &str) -> Result<SigningKey, AuthError> {
        let mut keys = self.keys.lock().await;

        if let Some(key) = keys.fresh(kid, self.config.jwks_ttl_seconds) {
            return Ok(key);
        }
        if !keys.may_refresh() {
            // Inside the cooldown: answer with what we have (or fail) instead of
            // fetching again, so an unknown kid cannot stampede the provider.
            return keys.entries.get(kid).cloned().ok_or(AuthError::Unknown);
        }

        self.refresh(&mut keys).await?;
        keys.entries.get(kid).cloned().ok_or(AuthError::Unknown)
    }

    /// Fetches discovery (once) and the key set.
    ///
    /// The caller holds the lock, which is what makes a concurrent burst of
    /// requests share one fetch.
    async fn refresh(&self, keys: &mut Keys) -> Result<(), AuthError> {
        keys.attempted_at = Some(Instant::now());

        let jwks_uri = if let Some(uri) = keys.jwks_uri.clone() {
            uri
        } else {
            // `unwrap_or` would evaluate `discover` eagerly, which both wastes a
            // request and breaks providers that have no discovery document at all.
            let uri = match &self.config.jwks_uri {
                Some(uri) => uri.clone(),
                None => self.discover(&self.issuer).await?,
            };
            keys.jwks_uri = Some(uri.clone());
            uri
        };

        let jwks: Jwks = self.fetch_json(&jwks_uri).await?;
        let mut refreshed = HashMap::new();
        for jwk in jwks.keys {
            if let Some((kid, key)) = signing_key(&jwk) {
                refreshed.insert(kid, key);
            }
        }
        if refreshed.is_empty() {
            return Err(AuthError::Unknown);
        }

        keys.entries = refreshed;
        keys.fetched_at = Some(Instant::now());
        Ok(())
    }

    /// Reads `{issuer}/.well-known/openid-configuration`.
    async fn discover(&self, issuer: &str) -> Result<String, AuthError> {
        let discovery: Discovery = self
            .fetch_json(&format!("{issuer}/.well-known/openid-configuration"))
            .await?;
        if discovery.jwks_uri.trim().is_empty() {
            return Err(AuthError::Unknown);
        }
        Ok(discovery.jwks_uri)
    }

    /// Fetches and decodes a JSON document.
    async fn fetch_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, AuthError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|_| AuthError::Unknown)?;
        if !response.status().is_success() {
            return Err(AuthError::Unknown);
        }
        response.json::<T>().await.map_err(|_| AuthError::Unknown)
    }

    /// The identity a validated claim set describes.
    fn identity(&self, claims: &Value) -> Result<Identity, AuthError> {
        let subject = claims
            .get(&self.config.subject_claim)
            .and_then(Value::as_str)
            .ok_or(AuthError::Unknown)?;

        Ok(Identity {
            user_id: self.subject_id(subject)?,
            is_admin: self.is_admin(claims),
            email: self.email(claims),
        })
    }

    /// Maps a `sub` onto an [`Id`].
    ///
    /// A canonical UUID is used as-is. Anything else needs a configured
    /// namespace, where the subject is derived (`UUIDv5`) so the same subject
    /// always yields the same id; without one the token is refused rather than
    /// guessed at.
    fn subject_id(&self, subject: &str) -> Result<Id, AuthError> {
        if let Ok(id) = Id::parse(subject) {
            return Ok(id);
        }
        let namespace = self.namespace.ok_or(AuthError::Unknown)?;
        Ok(Id::from(Key::new(&namespace, subject)))
    }

    /// The caller's email, when the provider marks it verified.
    ///
    /// `None` for a provider that does not issue one, and for an address the
    /// provider itself says is unverified — unless the deployment has turned
    /// [`OidcConfig::require_verified_email`] off, in which case the claim is
    /// taken at face value.
    fn email(&self, claims: &Value) -> Option<String> {
        let email = claims
            .get(&self.config.email_claim)
            .and_then(Value::as_str)?
            .trim();
        if email.is_empty() {
            return None;
        }
        if self.config.require_verified_email
            && claims.get("email_verified").and_then(Value::as_bool) != Some(true)
        {
            return None;
        }
        Some(email.to_owned())
    }

    /// Whether [`OidcConfig::admin_group`] is present in the configured claim.
    ///
    /// The claim path may address nested objects (`realm_access.roles`), and the
    /// value may be a single string or an array of strings.
    fn is_admin(&self, claims: &Value) -> bool {
        let claim = self
            .config
            .groups_claim
            .split('.')
            .try_fold(claims, |value, segment| value.get(segment));
        let admin = self.config.admin_group.as_str();

        match claim {
            Some(Value::Array(items)) => items
                .iter()
                .any(|item| item.as_str().is_some_and(|group| group == admin)),
            Some(Value::String(only)) => only == admin,
            _ => false,
        }
    }
}

impl Authenticator for OidcAuthenticator {
    fn authenticate(&self, token: Option<&str>) -> AuthFuture<'_> {
        let token = token.map(str::to_owned);

        Box::pin(async move {
            let token = token.ok_or(AuthError::Missing)?;

            let header = decode_header(&token).map_err(|_| AuthError::Unknown)?;
            let kid = header.kid.as_deref().ok_or(AuthError::Unknown)?;
            let (key, algorithm) = self.key(kid).await?;

            // The algorithm comes from the key, never from the token: a token
            // claiming a different one (e.g. `HS256` against RSA material) is
            // refused instead of being verified on the attacker's terms.
            if header.alg != algorithm {
                return Err(AuthError::Unknown);
            }

            let mut validation = Validation::new(algorithm);
            validation.leeway = self.config.leeway_seconds;
            validation.set_issuer(&[self.issuer()]);
            match &self.config.audience {
                Some(audience) => validation.set_audience(&[audience]),
                // The provider may not set an audience at all; requiring one
                // would reject otherwise valid tokens.
                None => validation.validate_aud = false,
            }

            let claims = decode::<Value>(&token, &key, &validation)
                .map_err(|_| AuthError::Unknown)?
                .claims;

            self.identity(&claims)
        })
    }
}

/// Builds the verification key and algorithm a JWK describes.
fn signing_key(jwk: &Jwk) -> Option<(String, SigningKey)> {
    let kid = jwk.kid.clone()?;

    let (key, algorithm) = match jwk.kty.as_str() {
        "RSA" => {
            let key =
                DecodingKey::from_rsa_components(jwk.n.as_deref()?, jwk.e.as_deref()?).ok()?;
            (
                key,
                match jwk.alg.as_deref() {
                    None | Some("RS256") => Algorithm::RS256,
                    Some("RS384") => Algorithm::RS384,
                    Some("RS512") => Algorithm::RS512,
                    Some("PS256") => Algorithm::PS256,
                    Some("PS384") => Algorithm::PS384,
                    Some("PS512") => Algorithm::PS512,
                    // An unexpected algorithm (an encryption key, or a signature
                    // scheme we will not accept) means the key is unusable to us.
                    Some(_) => return None,
                },
            )
        }
        "EC" => {
            let key = DecodingKey::from_ec_components(jwk.x.as_deref()?, jwk.y.as_deref()?).ok()?;
            let algorithm = match (jwk.crv.as_deref(), jwk.alg.as_deref()) {
                (Some("P-256"), None | Some("ES256")) => Algorithm::ES256,
                (Some("P-384"), None | Some("ES384")) => Algorithm::ES384,
                _ => return None,
            };
            (key, algorithm)
        }
        // `OKP` keys are skipped: the JWT library offers no component
        // constructor for Ed25519 public keys, and guessing at the encoding of a
        // signature key is not something to do silently.
        _ => return None,
    };

    Some((kid, (key, algorithm)))
}

/// Installs the workspace's rustls (`ring`) provider once per process.
///
/// `reqwest` is built with `rustls-no-provider`, so a process must install a
/// provider **before** building a `Client` (otherwise it panics). Every
/// authenticator here does this itself; the function is public so a host — or the
/// integration suite's HTTPS probe — can ensure it before using `reqwest`
/// directly. Installing twice is a no-op.
pub fn install_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A JWK fixture builder.
    fn jwk(kty: &str, alg: Option<&str>) -> Jwk {
        Jwk {
            kid: Some("key-1".to_owned()),
            kty: kty.to_owned(),
            alg: alg.map(str::to_owned),
            crv: None,
            n: Some("AQAB".to_owned()),
            e: Some("AQAB".to_owned()),
            x: Some("AQAB".to_owned()),
            y: Some("AQAB".to_owned()),
        }
    }

    #[test]
    fn rsa_algorithms_come_from_the_key() {
        assert_eq!(
            signing_key(&jwk("RSA", None)).map(|(kid, (_, algorithm))| (kid, algorithm)),
            Some(("key-1".to_owned(), Algorithm::RS256)),
            "an RSA key without `alg` is assumed to sign RS256"
        );
        assert!(signing_key(&jwk("RSA", Some("PS256"))).is_some());
        assert!(
            signing_key(&jwk("RSA", Some("RSA-OAEP"))).is_none(),
            "an encryption key is not a signing key"
        );
        assert!(signing_key(&jwk("RSA", Some("HS256"))).is_none());
    }

    #[test]
    fn ec_keys_map_their_curve_to_an_algorithm() {
        let mut p256 = jwk("EC", None);
        p256.crv = Some("P-256".to_owned());
        assert_eq!(
            signing_key(&p256).map(|(_, (_, algorithm))| algorithm),
            Some(Algorithm::ES256)
        );

        let mut p384 = jwk("EC", Some("ES384"));
        p384.crv = Some("P-384".to_owned());
        assert!(signing_key(&p384).is_some());

        let mut mismatched = jwk("EC", Some("ES384"));
        mismatched.crv = Some("P-256".to_owned());
        assert!(signing_key(&mismatched).is_none());

        let mut no_curve = jwk("EC", None);
        no_curve.crv = None;
        assert!(signing_key(&no_curve).is_none());
    }

    #[test]
    fn unusable_keys_are_skipped() {
        let mut okp = jwk("OKP", None);
        okp.crv = Some("Ed25519".to_owned());
        assert!(signing_key(&okp).is_none(), "OKP is documented as skipped");

        let mut no_kid = jwk("RSA", None);
        no_kid.kid = None;
        assert!(signing_key(&no_kid).is_none());
    }

    #[test]
    fn the_admin_claim_accepts_arrays_strings_and_nested_paths() {
        let authenticator = OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com".to_owned(),
            ..OidcConfig::default()
        })
        .unwrap();

        let claims = |json: &str| serde_json::from_str::<Value>(json).unwrap();
        assert!(authenticator.is_admin(&claims(r#"{"groups":["admins"]}"#)));
        assert!(authenticator.is_admin(&claims(r#"{"groups":"admins"}"#)));
        assert!(!authenticator.is_admin(&claims(r#"{"groups":["members"]}"#)));
        assert!(!authenticator.is_admin(&claims(r#"{"groups":null}"#)));
        assert!(!authenticator.is_admin(&claims("{}")));
    }

    #[test]
    fn a_nested_claim_path_is_followed() {
        let authenticator = OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com".to_owned(),
            groups_claim: "realm_access.roles".to_owned(),
            admin_group: "platform-admin".to_owned(),
            ..OidcConfig::default()
        })
        .unwrap();

        let claims =
            serde_json::from_str::<Value>(r#"{"realm_access":{"roles":["platform-admin"]}}"#)
                .unwrap();
        assert!(authenticator.is_admin(&claims));
    }

    #[test]
    fn a_subject_is_a_uuid_when_the_provider_says_so() {
        let authenticator = OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com".to_owned(),
            ..OidcConfig::default()
        })
        .unwrap();

        let id = authenticator
            .subject_id("018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9")
            .unwrap();
        assert_eq!(id.to_string(), "018f2c3d-4e5f-7071-8293-a4b5c6d7e8f9");
        assert_eq!(
            authenticator.subject_id("opaque-subject"),
            Err(AuthError::Unknown),
            "without a namespace an opaque subject is refused, not guessed"
        );
    }

    #[test]
    fn an_opaque_subject_becomes_a_stable_derived_id() {
        let authenticator = OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com".to_owned(),
            subject_namespace: Some("5c1a9f43-2e78-4b06-8d17-a2c3b4d5e6f7".to_owned()),
            ..OidcConfig::default()
        })
        .unwrap();

        let first = authenticator.subject_id("opaque-subject").unwrap();
        let again = authenticator.subject_id("opaque-subject").unwrap();
        let other = authenticator.subject_id("another-subject").unwrap();
        assert_eq!(first, again, "the same subject always maps to the same id");
        assert_ne!(first, other);
    }

    #[test]
    fn a_bad_namespace_is_refused_at_construction() {
        let error = OidcAuthenticator::new(OidcConfig {
            issuer: "https://idp.example.com".to_owned(),
            subject_namespace: Some("not-a-uuid".to_owned()),
            ..OidcConfig::default()
        })
        .err()
        .expect("a rejected namespace");
        assert!(error.to_string().contains("subject_namespace"), "{error}");
    }
}
