//! A transactional filesystem cache layer based on [`freqfs`].
//! See the "examples" directory for usage examples.

use std::fmt;
use std::io;

pub use hr_id::Id;

pub use dir::{Dir, DirEntry, Key, VERSIONS};
pub use file::{File, FileVersionRead};

mod dir;
mod file;

#[cfg(test)]
#[path = "../tests/unit/lifecycle.rs"]
pub(crate) mod lifecycle_tests;

/// An error encountered during a transactional filesystem operation
pub enum Error {
    Conflict(txn_lock::Error),
    Corrupt(String),
    IO(io::Error),
    NotFound(String),
    Parse(hr_id::ParseError),
}

impl From<hr_id::ParseError> for Error {
    fn from(cause: hr_id::ParseError) -> Self {
        Self::Parse(cause)
    }
}

impl From<io::Error> for Error {
    fn from(cause: io::Error) -> Self {
        Self::IO(cause)
    }
}

impl From<txn_lock::Error> for Error {
    fn from(cause: txn_lock::Error) -> Self {
        Self::Conflict(cause)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Conflict(cause) => cause.fmt(f),
            Self::Corrupt(cause) => write!(f, "corrupt transactional storage: {cause}"),
            Self::IO(cause) => cause.fmt(f),
            Self::NotFound(locator) => write!(f, "not found: {locator}"),
            Self::Parse(cause) => cause.fmt(f),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conflict(cause) => Some(cause),
            Self::IO(cause) => Some(cause),
            Self::Parse(cause) => Some(cause),
            Self::Corrupt(_) | Self::NotFound(_) => None,
        }
    }
}

/// The result of a transactional filesystem operation
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[path = "../tests/unit/roundtrip.rs"]
mod tests;
