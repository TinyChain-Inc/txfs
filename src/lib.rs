//! A transactional filesystem cache layer based on [`freqfs`].
//! See the "examples" directory for usage examples.

use std::{fmt, io};

pub use dir::{Dir, DirEntry, Key, VERSIONS};
pub use file::{File, FileVersionRead, FileVersionWrite};
pub use hr_id::Id;

mod dir;
mod file;

/// An error encountered during a transactional filesystem operation
pub enum Error {
    Conflict(txn_lock::Error),
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

impl std::error::Error for Error {}

/// The result of a transactional filesystem operation
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::fmt;
    use std::time::{SystemTime, UNIX_EPOCH};

    use freqfs::Cache;
    use freqfs::Name;
    use get_size::GetSize;
    use safecast::as_type;
    use safecast::AsType;
    use tokio::fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
    struct Txn(u64);

    impl fmt::Display for Txn {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    impl PartialEq<str> for Txn {
        fn eq(&self, other: &str) -> bool {
            if let Ok(other) = other.parse() {
                self.0 == other
            } else {
                false
            }
        }
    }

    impl PartialOrd<str> for Txn {
        fn partial_cmp(&self, other: &str) -> Option<Ordering> {
            if let Ok(other) = other.parse() {
                PartialOrd::partial_cmp(&self.0, &other)
            } else {
                None
            }
        }
    }

    impl Name for Txn {
        fn partial_cmp(&self, key: &str) -> Option<Ordering> {
            Name::partial_cmp(&self.0, key)
        }
    }

    #[derive(Clone)]
    enum Entry {
        Bin(Vec<u8>),
    }

    impl freqfs::FileLoad for Entry {
        async fn load(
            _path: &std::path::Path,
            mut file: tokio::fs::File,
            _metadata: std::fs::Metadata,
        ) -> std::io::Result<Self> {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;
            Ok(Self::Bin(bytes))
        }
    }

    impl freqfs::FileSave for Entry {
        async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
            match self {
                Self::Bin(bytes) => {
                    file.write_all(bytes).await?;
                    Ok(bytes.len() as u64)
                }
            }
        }
    }

    impl GetSize for Entry {
        fn get_size(&self) -> usize {
            match self {
                Self::Bin(bytes) => bytes.get_size(),
            }
        }
    }

    impl AsType<Entry> for Entry {
        fn as_type(&self) -> Option<&Entry> {
            Some(self)
        }

        fn as_type_mut(&mut self) -> Option<&mut Entry> {
            Some(self)
        }

        fn into_type(self) -> Option<Entry> {
            Some(self)
        }
    }

    as_type!(Entry, Bin, Vec<u8>);

    #[test]
    fn name_partial_cmp_accepts_str() {
        let id = Txn(5);

        assert_eq!(Name::partial_cmp(&id, "5"), Some(Ordering::Equal));
        assert_eq!(Name::partial_cmp(&id, "7"), Some(Ordering::Less));
        assert_eq!(Name::partial_cmp(&id, "nope"), None);
    }

    #[tokio::test]
    async fn file_roundtrip_persists_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        path.push(format!("txfs_test_{}_{}", std::process::id(), unique));
        fs::create_dir(&path).await?;

        let cache = Cache::<Entry>::new(40, None);
        let root = cache.load(path.clone())?;
        let dir = super::Dir::load(Txn(1), root).await?;

        let name: super::Id = "file-one".parse()?;
        let file = dir
            .create_file(Txn(1), name, Entry::Bin(vec![1u8, 2, 3]))
            .await?;

        let read = file.read::<Entry>(Txn(1)).await?;
        match &*read {
            Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[1u8, 2, 3]),
        }

        {
            let mut write = file.write::<Entry>(Txn(2)).await?;
            *write = Entry::Bin(vec![9u8, 8]);
        }

        file.commit(Txn(2)).await;

        let read = file.read::<Entry>(Txn(3)).await?;
        match &*read {
            Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[9u8, 8]),
        }

        let _ = fs::remove_dir_all(&path).await;

        Ok(())
    }
}
