// SPDX-License-Identifier: MPL-2.0

//! Edge pre-computation: blocking-but-pure work before a command enters
//! consensus (`design.md` §2.2, principle 7).
//!
//! Password hashing is the canonical example. The gateway replaces a request's
//! plaintext `password` with an argon2 `password_hash`, so the plaintext never
//! reaches the replicated log, and every replica sees only the derived value.
//! The step is pure: the same input password and salt produce the same hash, and
//! nothing here touches consensus or storage.
//!
//! Verification is the same primitive in reverse: [`verify_password`] checks a
//! presented password against a stored hash. A future credential command
//! consumes `password_hash`; today the gateway guarantees it is what enters
//! consensus.

use argon2::Argon2;
use argon2::password_hash::PasswordHash;
use argon2::password_hash::PasswordHasher;
use argon2::password_hash::PasswordVerifier;
use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use serde_json::Value;

/// The request field carrying a plaintext password.
pub const PASSWORD_FIELD: &str = "password";

/// The command-payload field carrying the argon2 hash.
pub const PASSWORD_HASH_FIELD: &str = "password_hash";

/// Why pre-computation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PreComputeError {
    /// The password could not be hashed.
    #[error("the password could not be hashed")]
    Hash,
    /// The stored hash is malformed.
    #[error("the stored password hash is malformed")]
    MalformedHash,
}

/// Hashes a payload's `password` field into `password_hash` in place.
///
/// A payload without a `password` field (or that is not a JSON object) is left
/// untouched, so this is safe to run on every command.
///
/// # Errors
///
/// [`PreComputeError::Hash`] if argon2 refuses the input.
pub fn hash_password(payload: &mut Value) -> Result<(), PreComputeError> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(());
    };

    let Some(password) = object
        .get(PASSWORD_FIELD)
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(());
    };

    object.remove(PASSWORD_FIELD);

    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| PreComputeError::Hash)?
        .to_string();

    object.insert(PASSWORD_HASH_FIELD.to_owned(), Value::String(hash));
    Ok(())
}

/// Verifies a presented password against a stored argon2 hash.
///
/// # Errors
///
/// [`PreComputeError::MalformedHash`] if `hash` is not a PHC argon2 string.
pub fn verify_password(password: &str, hash: &str) -> Result<bool, PreComputeError> {
    let parsed = PasswordHash::new(hash).map_err(|_| PreComputeError::MalformedHash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_password_is_replaced_by_a_verifiable_hash() {
        let mut payload = json!({ "title": "a task", "password": "s3cret" });

        hash_password(&mut payload).unwrap();

        assert!(
            payload.get(PASSWORD_FIELD).is_none(),
            "plaintext must not survive"
        );
        let hash = payload
            .get(PASSWORD_HASH_FIELD)
            .and_then(Value::as_str)
            .unwrap();
        assert!(hash.starts_with("$argon2"), "argon2 PHC string expected");
        assert!(verify_password("s3cret", hash).unwrap());
        assert!(!verify_password("wrong", hash).unwrap());
        assert_eq!(payload.get("title").and_then(Value::as_str), Some("a task"));
    }

    #[test]
    fn a_payload_without_a_password_is_untouched() {
        let mut payload = json!({ "title": "a task" });
        let before = payload.clone();
        hash_password(&mut payload).unwrap();
        assert_eq!(payload, before);
    }

    #[test]
    fn a_malformed_hash_is_an_error_not_a_false_match() {
        assert_eq!(
            verify_password("x", "not a hash"),
            Err(PreComputeError::MalformedHash)
        );
    }

    #[test]
    fn hashing_is_salted_so_equal_passwords_differ() {
        let mut first = json!({ "password": "same" });
        let mut second = json!({ "password": "same" });
        hash_password(&mut first).unwrap();
        hash_password(&mut second).unwrap();
        assert_ne!(first, second, "each hash carries its own salt");
    }
}
