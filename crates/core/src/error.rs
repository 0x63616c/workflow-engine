//! The error type that crosses every boundary (state machine replies, gateway
//! responses, SDK results). It is data: serializable, comparable, stable.

use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ErrorCode {
    /// The request is malformed or violates a documented limit.
    InvalidArgument,
    /// The named thing does not exist.
    NotFound,
    /// The named thing already exists (for example a running workflow id).
    AlreadyExists,
    /// The request is valid but the current state does not allow it.
    FailedPrecondition,
    /// The task or token is stale: the state has moved on. Safe to drop.
    Stale,
    /// This cell is a standby; writes must go to the active cell.
    NotActive,
    /// Transient: retry, possibly against another node.
    Unavailable,
    /// Backpressure: retry later.
    ResourceExhausted,
    /// The caller's deadline passed before the operation finished.
    DeadlineExceeded,
    /// Workflow code diverged from recorded history.
    Nondeterminism,
    /// Not supported (yet).
    Unimplemented,
    /// A bug or an invariant violation on the server side.
    Internal,
}

impl ErrorCode {
    /// Whether a client may transparently retry the same request.
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorCode::Unavailable | ErrorCode::ResourceExhausted | ErrorCode::DeadlineExceeded)
    }

    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{}: {message}", code.as_str())]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Error {
        Error { code, message: message.into() }
    }
    pub fn invalid(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::InvalidArgument, message)
    }
    pub fn not_found(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::NotFound, message)
    }
    pub fn already_exists(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::AlreadyExists, message)
    }
    pub fn precondition(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::FailedPrecondition, message)
    }
    pub fn stale(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::Stale, message)
    }
    pub fn not_active(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::NotActive, message)
    }
    pub fn unavailable(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::Unavailable, message)
    }
    pub fn exhausted(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::ResourceExhausted, message)
    }
    pub fn deadline(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::DeadlineExceeded, message)
    }
    pub fn unimplemented(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::Unimplemented, message)
    }
    pub fn internal(message: impl Into<String>) -> Error {
        Error::new(ErrorCode::Internal, message)
    }

    /// `InvalidArgument` naming every empty field of `request`, so the caller
    /// fixes them all in one round trip. `fields` is `(name, value)` pairs.
    pub fn require_fields(request: &str, fields: &[(&str, &str)]) -> Result<()> {
        let missing: Vec<&str> = fields.iter().filter(|(_, value)| value.is_empty()).map(|(name, _)| *name).collect();
        if missing.is_empty() {
            return Ok(());
        }
        let required: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
        Err(Error::invalid(format!(
            "{request} has empty {}: set every required field ({})",
            missing.join(", "),
            required.join(", ")
        )))
    }
}

impl From<crate::codec::DecodeError> for Error {
    fn from(e: crate::codec::DecodeError) -> Error {
        Error::internal(e.to_string())
    }
}
