// SPDX-License-Identifier: MPL-2.0

//! Keycloak/OIDC authentication — the `test-services` implementation.
//!
//! Validates a bearer token against a realm's `userinfo` endpoint and maps the
//! `groups` claim onto [`Identity::is_admin`]. The integration suite points it
//! at the compose Keycloak (`mise run test-services`); a deployment points the
//! same type at its own realm.

use loomery_core::id::Id;
use serde::Deserialize;

use super::AuthError;
use super::AuthFuture;
use super::Authenticator;
use super::Identity;

/// The group that carries the system-admin claim by default.
pub const DEFAULT_ADMIN_GROUP: &str = "admins";

/// Authenticates bearer tokens against a Keycloak realm.
pub struct KeycloakAuthenticator {
    http: reqwest::Client,
    userinfo_url: String,
    admin_group: String,
}

impl KeycloakAuthenticator {
    /// Builds an authenticator for `{base_url}/realms/{realm}`.
    #[must_use]
    pub fn new(base_url: &str, realm: &str) -> Self {
        super::oidc::install_tls_provider();

        Self {
            http: reqwest::Client::new(),
            userinfo_url: format!(
                "{}/realms/{realm}/protocol/openid-connect/userinfo",
                base_url.trim_end_matches('/')
            ),
            admin_group: DEFAULT_ADMIN_GROUP.to_owned(),
        }
    }

    /// Uses `group` as the system-admin claim instead of [`DEFAULT_ADMIN_GROUP`].
    #[must_use]
    pub fn with_admin_group(mut self, group: &str) -> Self {
        group.clone_into(&mut self.admin_group);
        self
    }

    /// Exchanges a username/password for an access token (the `OAuth2` password
    /// grant).
    ///
    /// Offered so the integration suite can obtain a real token from the test
    /// realm without carrying its own HTTP client.
    ///
    /// # Errors
    ///
    /// The endpoint's error, a rejected credential, or a malformed response.
    pub async fn password_token(
        base_url: &str,
        realm: &str,
        client_id: &str,
        username: &str,
        password: &str,
    ) -> anyhow::Result<String> {
        super::oidc::install_tls_provider();

        let response = reqwest::Client::new()
            .post(format!(
                "{}/realms/{realm}/protocol/openid-connect/token",
                base_url.trim_end_matches('/')
            ))
            .form(&[
                ("grant_type", "password"),
                ("client_id", client_id),
                ("username", username),
                ("password", password),
                ("scope", "openid"),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;

        response
            .get("access_token")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("the token response had no access_token"))
    }
}

impl KeycloakAuthenticator {
    /// Fetches the `userinfo` document for `token`.
    async fn userinfo(&self, token: &str) -> Result<UserInfo, AuthError> {
        let response = self
            .http
            .get(&self.userinfo_url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|_| AuthError::Unknown)?;

        if !response.status().is_success() {
            return Err(AuthError::Unknown);
        }

        response.json().await.map_err(|_| AuthError::Unknown)
    }
}

/// The subset of the `userinfo` response the gateway needs.
#[derive(Debug, Deserialize)]
struct UserInfo {
    sub: String,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    email_verified: bool,
}

impl Authenticator for KeycloakAuthenticator {
    fn authenticate(&self, token: Option<&str>) -> AuthFuture<'_> {
        let token = token.map(str::to_owned);

        Box::pin(async move {
            let token = token.ok_or(AuthError::Missing)?;
            let userinfo = self.userinfo(&token).await?;
            let user_id = Id::parse(&userinfo.sub).map_err(|_| AuthError::Unknown)?;
            let is_admin = userinfo
                .groups
                .iter()
                .any(|group| group == &self.admin_group);

            // The address is attribution only when the provider vouches for it.
            let email = userinfo
                .email
                .filter(|_| userinfo.email_verified)
                .map(|email| email.trim().to_owned())
                .filter(|email| !email.is_empty());

            Ok(Identity {
                user_id,
                is_admin,
                email,
            })
        })
    }
}
