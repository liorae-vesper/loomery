// SPDX-License-Identifier: MPL-2.0

//! Domain errors — the pure core's `Result::Err` type.
//!
//! [`DomainError`] is generic over its **code** type `C`, so each part of the
//! app defines its own error-code enum (the core itself, each aggregate, the
//! shell) and the error type stays shared. The `message` is human-readable;
//! the `code` is what machines should switch on.

use thiserror::Error;

/// An application-level domain error.
///
/// `code` carries a machine-readable, comparable discriminator; `message` is
/// for humans and logs; `cause` optionally chains the lower-level error that
/// produced this domain error (shell-side plumbing — the pure core treats
/// errors as code + message).
#[derive(Debug, Error)]
#[error("{message}")]
pub struct DomainError<C> {
    /// Machine-readable error discriminator (an enum per domain area).
    pub code: C,
    /// Human-readable description.
    pub message: String,
    /// The lower-level error that caused this one, if any.
    #[source]
    pub cause: Option<anyhow::Error>,
}

impl<C> DomainError<C> {
    /// Builds a domain error with no underlying cause.
    #[must_use]
    pub fn new(code: C, message: &str) -> Self {
        DomainError {
            code,
            message: message.to_string(),
            cause: None,
        }
    }

    /// Builds a domain error with an underlying cause attached.
    #[must_use]
    pub fn with_cause(code: C, message: &str, cause: Option<anyhow::Error>) -> Self {
        DomainError {
            code,
            message: message.to_string(),
            cause,
        }
    }
}

impl<C: PartialEq> PartialEq for DomainError<C> {
    /// Errors compare equal by `code` and `message` — the cause chain is
    /// explicitly ignored (it is `anyhow::Error`, which is not comparable).
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && self.message == other.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    /// A stand-in for an app-defined code enum, mirroring how each part of
    /// the app will bring its own.
    #[derive(Debug, PartialEq)]
    enum Code {
        NotFound,
        Invalid,
    }

    #[test]
    fn display_is_the_message() {
        let err = DomainError::new(Code::NotFound, "task not found");
        assert_eq!(err.to_string(), "task not found");
    }

    #[test]
    fn equality_compares_code_and_message() {
        let a = DomainError::new(Code::NotFound, "nope");
        let b = DomainError::new(Code::NotFound, "nope");
        assert_eq!(a, b);

        let c = DomainError::new(Code::Invalid, "nope");
        assert_ne!(a, c);
    }

    #[test]
    fn cause_chains_to_source() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "fs boom");
        let err = DomainError::with_cause(
            Code::NotFound,
            "task not found",
            Some(anyhow::Error::new(inner)),
        );
        assert!(err.source().is_some());
        assert_eq!(err.source().unwrap().to_string(), "fs boom");
    }
}
